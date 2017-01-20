import os
import sys
import json
import traceback
import requests
import threading
import logging

import gevent
from gevent import monkey

import SocketServer
import BaseHTTPServer
from urlparse import urlparse, parse_qsl
from urllib import urlencode
from SimpleHTTPServer import SimpleHTTPRequestHandler

from peer.utils import TokenBucket, elapsed_time, sizeof_fmt_human
from peer.utils import gevent_sendfile

monkey.patch_select()
logger = logging.getLogger('peer.server')


class ResourceManager(dict):
    def add_res(self, res_dest, local_path):
        self[res_dest] = local_path

    def del_res(self, res_dest):
        del self[res_dest]


res_mng = ResourceManager()


class ServiceManager(object):
    def __init__(self):
        self.res_mng = res_mng
        SocketServer.TCPServer.allow_reuse_address = True
        self.httpd = None
        self.server_running = False
        self._lock = threading.RLock()

    def is_server_running(self):
        return self.server_running

    def run_server(self, rate=None):
        with self._lock:
            if self.server_running:
                return
            self.httpd = GeventServer(('0.0.0.0', 0), PeerHandler)
            t = threading.Thread(
                target=self.httpd.serve_forever,
                kwargs={'poll_interval': 0.01})
            t.start()
            self.server_running = True

    def shutdown_server(self):
        with self._lock:
            if self.httpd:
                self.httpd.shutdown()
                self.server_running = False

    def get_httpd_server(self):
        return self.httpd

    def add_res(self, res_dest, local_path):
        with self._lock:
            self.res_mng.add_res(res_dest, local_path)

    def del_res(self, res_dest):
        with self._lock:
            self.res_mng.del_res(res_dest)
            if not self.res_mng.keys():
                self.shutdown_server()


service_manager = ServiceManager()


def set_upload_rate(rate=None):
    if rate:
        PeerHandler.token_bucket = TokenBucket(rate)
    else:
        PeerHandler.token_bucket = None


class ConnInfo(object):
    def __init__(self):
        self.conn_num = 0
        self._lock = threading.Lock()

    def conn_increase(self):
        with self._lock:
            self.conn_num += 1

    def conn_decrease(self):
        with self._lock:
            self.conn_num -= 1


class ThreadingsServer(SocketServer.ThreadingMixIn,
                       BaseHTTPServer.HTTPServer):

    request_queue_size = 1024


class GeventMixIn:
    def process_request_thread(self, request, client_address):
        try:
            self.finish_request(request, client_address)
            self.shutdown_request(request)
        except:
            self.handle_error(request, client_address)
            self.shutdown_request(request)

    def process_request(self, request, client_address):
        gevent.spawn(self.process_request_thread, request, client_address)


class PeerHandler(SimpleHTTPRequestHandler):

    protocol_version = "HTTP/1.1"

    res_mng = None
    token_bucket = None
    conn_info = None

    rbufsize = 4194304
    wbufsize = 4194304

    def log_message(self, format, *args):
        logger.info("%s %s- - [%s] %s" %
                    (self.client_address[0], self.client_address[1],
                        self.log_date_time_string(),
                        format % args)
                    )

    def do_GET(self):
        ret = None
        with elapsed_time() as p:
            self.conn_info.conn_increase()
            try:
                logger.info('handler conn_info: %s' % self.conn_info.conn_num)
                if self.token_bucket:
                    logger.info('Server network rate: %s/s' %
                                sizeof_fmt_human(self.token_bucket.get_rate()))
                ret = self._do_GET()
            except Exception as e:
                ex_type, ex, tb = sys.exc_info()
                logger.error(
                    'PeerHandler Error %s, traceback: %s' %
                    (
                        ex, traceback.format_list(
                            traceback.extract_tb(tb))
                    )
                )
                self.send_error(500, "inner error %s" % type(e))
            finally:
                self.conn_info.conn_decrease()

        logger.debug('access %s use time: %.4f' % (self.path, p.elapsed_time))
        if p.elapsed_time > 0.4:
            logger.debug(self.headers["range"])
        return ret

    def _do_GET(self):
        self.parse_param()
        res_url = self.GET.get('res_url', None)
        pieces = self.GET.get('pieces', None)

        realpath = None
        if res_url:
            try:
                piecefile = self.res_mng[res_url]
            except KeyError:
                self.send_error(404, "File not found")
                return
        else:
            # error raise
            self.send_error(404, "res_url should be specified")
            return

        if pieces:
            ret = {'status': 'normal'}

            if pieces == 'all':
                if self.token_bucket and\
                   self.token_bucket.get_rate_usage() > 0.75:
                        ret['status'] = 'overload'

                else:
                    ret['result'] = piecefile.get_pieces_avail()
            else:
                pieces_list = pieces.split(',')
                ret['result'] = piecefile.get_pieces_avail(pieces_list)

            json_result = json.dumps(ret)
            self.send_200_head(len(json_result))
            self.wfile.write(json_result)
            return

        realpath = piecefile.filepath

        self.byte_ranges = []
        f = self.send_head(realpath, piecefile)

        # sendfile will skip user space buffer
        self.wfile.flush()
        chunksize = 4096 * 2 * 16
        if f:
            if self.byte_ranges:
                wfileno = self.wfile.fileno()
                rfileno = f.fileno()
                for start, end in self.byte_ranges:

                    left_size = end - start + 1
                    while True:
                        if left_size > 0:
                            tmp_chunksize = min(left_size, chunksize)
                            if self.token_bucket:
                                while not self.token_bucket.consume(
                                   tmp_chunksize):
                                    gevent.sleep(0.02)

                            gevent_sendfile(
                                wfileno, rfileno, start, tmp_chunksize)
                            left_size -= tmp_chunksize
                            start += tmp_chunksize
                        else:
                            break
            else:
                self.copyfile(f, self.wfile)
            f.close()

    def parse_param(self):
        self.GET = {}
        query = urlparse(self.path).query
        qs = parse_qsl(query)
        if qs:
            for k, v in qs:
                self.GET[k] = v
        return True

    def _do_range(self, f, piecefile=None):
        # byte_range: bytes=1-199,2-33
        byte_range = self.headers['range']

        byte_unit, file_range = byte_range.split('=')
        parts = file_range.split(',')
        t_length = 0
        md5_list = []
        for part in parts:
            range_list = part.split('-')
            if len(range_list) == 2:
                range_pair = range_list

                fs = os.fstat(f.fileno())
                if piecefile:
                    content_len = str(piecefile.max_len)
                else:
                    content_len = str(fs[6])

                start, end = range_pair
                start = int(start)
                end = int(end)
                self.byte_ranges.append((start, end))

                length = end - start + 1
                t_length += length

                md5 = None
                if piecefile:
                    piece_id = start / piecefile.piece_size
                    md5 = piecefile.get_piece_md5(piece_id)

                if md5:
                    md5_list.append(md5)
                else:
                    md5_list.append("")
            else:
                pass

        self.send_response(206)
        self.send_header("Content-Length", t_length)
        self.send_header("Content-Range", byte_range + '/' + content_len)
        if md5:
            self.send_header("Content-MD5", ",".join(md5_list))

        if self.token_bucket and \
           self.token_bucket.get_rate_usage() > 0.75:
            self.send_header("Minions-Status", "overload")
        self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
        self.end_headers()
        return f

    def send_200_head(self, length):
        self.send_response(200)
        self.send_header("Location", self.path + "/")
        self.send_header("Content-Length", length)
        self.end_headers()
        return None

    def send_head(self, realpath=None, piecefile=None):
        if not realpath:
            path = self.translate_path(self.path)
        else:
            path = realpath
        f = None
        if os.path.isdir(path):
            if not self.path.endswith('/'):
                # redirect browser - doing basically what apache does
                self.send_response(301)
                self.send_header("Location", self.path + "/")
                self.end_headers()
                return None
            for index in "index.html", "index.htm":
                index = os.path.join(path, index)
                if os.path.exists(index):
                    path = index
                    break
            else:
                return self.list_directory(path)
        ctype = self.guess_type(path)
        try:
            # Always read in binary mode. Opening files in text mode may cause
            # newline translations, making the actual size of the content
            # transmitted *less* than the content-length!
            f = open(path, 'rb')
        except IOError:
            self.send_error(404, "File not found")
            return None

        if self.headers.get('range', None):
            return self._do_range(f, piecefile)
        else:
            fs = os.fstat(f.fileno())
            content_len = str(fs[6])

            self.send_response(200)
            self.send_header("Content-type", ctype)
            self.send_header("Content-Length", content_len)
            self.send_header("Last-Modified",
                             self.date_time_string(fs.st_mtime))
            self.end_headers()
            return f


class GeventServer(GeventMixIn, BaseHTTPServer.HTTPServer):
    request_queue_size = 1024


def handler_factory(res_mng, rate):
    newclass = type("NewHanlder", (PeerHandler, object), {})
    newclass.res_mng = res_mng
    newclass.conn_info = ConnInfo()
    if rate:
        newclass.token_bucket = TokenBucket(rate)
    return newclass


if __name__ == '__main__':
    sm = ServiceManager()
    res_url = 'http://yum.tbsite.net/bigfile'
    sm.add_res(res_url, '/tmp/bigfile')
    sm.run_server()

    ip, port = sm.get_httpd_server().socket.getsockname()
    print 'server in %s:%s' % (ip, port)
    try:
        requests.get('http://localhost:%s?%s' %
                     (port, urlencode({'res_url': res_url})),
                     headers={"Range": "bytes=0-1"}
                     )
    finally:
        sm.shutdown_server()
