
import os
import json
import time
import errno
import socket
import random
import logging
import threading
import SocketServer
import sched
from hashlib import md5
from urllib import urlencode
from libs.mrequests import requests
from requests.exceptions import ConnectionError,\
    Timeout, HTTPError, ReadTimeout

import gevent
import gevent.pool

from models import PieceFile, ExpireStorage, ExcThread
from server import ResourceManager, handler_factory,\
    GeventServer

from threading import Thread, Lock
from utils import sizeof_fmt_human, TokenBucket,\
        get_res_length, http_download_to_piecefile,\
        elapsed_time, pyinotify, is_gaierror, ListStore,\
        generate_range_string
from excepts import NoPeersFound, TrackerUnavailable,\
        UnknowStrict, PeerOverload, OriginURLConnectError,\
        PieceChecksumError, IncompleteRead, RateTooSlow

reload(requests)

THREADS = 100
PROTOCOL = 'http://'
PORT = 9999
PIECE_SIZE = 1024

SHORT_BLACK_TIME = 3
LONG_BLACK_TIME = 30

GET_PEER_INTERVAL = 0.4

logger = logging.getLogger('minions')


class Minions(object):
    TIME_TO_TRACKER = 60
    logger = logger

    def __init__(self,
                 res_url,
                 download_dest=None,
                 tracker=None,
                 upload_res_url=None,
                 upload_filepath=None,
                 download_rate=None,
                 upload_rate=None,
                 strict=None,
                 fallback=False,
                 disorder=True):
        """
        @res_url origin url of resource
        @upload_res_url resource url in tracker or peers,
            which is not exactly as res_url sometime
        """

        self.res_url = res_url
        self.download_dest = download_dest

        self.fallback = fallback
        self.download_rate = download_rate
        self.upload_rate = upload_rate
        self.strict = strict
        if upload_res_url:
            self.upload_res_url = upload_res_url
        else:
            self.upload_res_url = res_url

        if upload_filepath:
            self.upload_filepath = upload_filepath
        else:
            self.upload_filepath = download_dest

        if not tracker:
            raise ValueError("tracker required")

        self.tracker = tracker
        self.loader = None
        self._ex_ip = None
        self._adt_info = None
        self.port = None
        self._is_activated = True
        self.download_thread = None
        self.uploader = None
        self.token_bucket = None

        self.res_is_downloaded = False
        self.disorder = disorder

        self._peer_in_conn = set()

        self.piece_file = None
        self.notifier = None

        self._black_peerlist = ExpireStorage(expire_time=30)
        self._black_lock = Lock()

        self._lock = Lock()

        self.ready_upload_thread = None
        self._is_uploading = False
        self._wait_uploading = False

        # multiplexing session would be good for performance
        self._requests_session = requests.Session()

    def activate(self):
        with self._lock:
            self._is_activated = True

    def deactivate(self):
        with self._lock:
            self._is_activated = False

    def is_activated(self):
        return self._is_activated

    def close(self):
        self.deactivate()
        self.stop_upload()
        if self.notifier:
            self.stop_pyinotify()

    def is_uploading(self):
        if self.uploader and self._is_uploading:
            return True
        else:
            return False

    def is_wait_uploading(self):
        return self._wait_uploading

    # @mprofile
    def get_peers(self):
        count = 0
        retry_count = 0
        max_retry = 3
        if self.strict:
            adt_info = self._get_strict_info()

        if self.strict:
            res_url = PROTOCOL + self.tracker + "/peer/?%s&strict=%s" %\
                (urlencode({'res': self.upload_res_url}), self.strict)
        else:
            res_url = PROTOCOL + self.tracker + "/peer/?%s" %\
                (urlencode({'res': self.upload_res_url}))

        logger.info('Try get peers for getting resouces %s from %s' %
                    (self.upload_res_url, res_url))

        while True:
            try:
                if self.strict:
                    r = self._requests_session.get(
                        res_url,
                        data=json.dumps({'adt_info': adt_info}))
                else:
                    r = self._requests_session.get(res_url)

                if not r.ok:
                    r.raise_for_status()

                logger.debug(r.content)

                ret = r.json()
                peers = ret[self.upload_res_url]
            except ConnectionError as e:
                if is_gaierror(e):
                    logger.error(
                        "resolve tracker server domain name : %s error" %
                        self.tracker)
                    raise e.args[0][1]
                else:
                    logger.error('get peers from tracker error: %s' % e)
                    raise TrackerUnavailable

            except HTTPError as e:
                logger.warn(
                    'get peers from tracker code error code: %s, msg: %s' %
                    (e.response.status_code, e.message))
                retry_count += 1
                if retry_count >= max_retry:
                    logger.error('max retry times run out')
                    raise TrackerUnavailable

                continue

            peers = [(ip, port) for ip, port in peers]
            for peer in peers[:]:
                # delete myself
                ip, port = peer
                peer_set = peer
                if (self._ex_ip, self.port) == peer_set:
                    peers.remove(peer)

                if peer_set in self._black_peerlist:
                    logger.debug('peer: %s:%s is in blacklist' % (ip, port))
                    peers.remove(peer)

            if self._ex_ip:
                if (self._ex_ip, self.port) in peers:
                    peers.remove((self._ex_ip, self.port))

            peers_num = len(peers)
            if len(peers) == 0 or peers == "" or \
                    peers is None or peers == 'null':
                logger.warn(
                    'Did not get any peers for %s' %
                    self.upload_res_url)
                count += 1
                if count >= peers_num:
                    raise NoPeersFound
            else:
                self.peers = peers
                logger.info('Get peers: %s from tracker' % peers)
                return peers

    def _listen_on_file(self):
        logger.debug('Listen on download file')
        s = sched.scheduler(time.time, time.sleep)

        def func():
            if self.piece_file.has_unalloc() and self.is_activated():
                logger.info("Already download %s" %
                            self.piece_file.get_real_filesize())
                s.enter(5, 1, func, ())
        s.enter(5, 1, func, ())

        Thread(target=s.run).start()

    def res_downloaded_size(self):
        if self.piece_file:
            return self.piece_file.get_real_filesize()
        else:
            return 0

    def download_res(
            self,
            rate=None,
            callback=None,
            cb_kwargs=None,
            thread=False):
        if thread:
            self.download_thread = ExcThread(
                    target=self._download_res,
                    kwargs={
                        'filepath': self.download_dest,
                        'rate': rate,
                        'callback': callback,
                        'cb_kwargs': cb_kwargs,
                        }
                    )
            self.download_thread.start()
        else:
            self._download_res(
                filepath=self.download_dest,
                rate=rate,
                callback=callback,
                cb_kwargs=cb_kwargs
            )

    def _record_get_peer_ts(self):
        self._last_get_peer_time = time.time()

    def _get_last_get_peer_tv(self):
        return time.time() - self._last_get_peer_time

    def _download_res(
            self,
            filepath,
            rate,
            uploading=True,
            callback=None,
            cb_kwargs=None):
        try:
            peers = self.get_peers()
            self._record_get_peer_ts()
            peers_num = len(peers)
            count = 0

            # just get resource size
            while True:
                ip, port = peers[count]
                logger.info('get resource size')
                try:
                    ret = self._requests_session.get(
                        "{protocol}{ip}:{port}/?{res}"
                        .format(
                            protocol=PROTOCOL, ip=ip,
                            port=port,
                            res=urlencode({'res_url': self.upload_res_url})),
                        stream=True,
                        headers={"Range": "bytes=0-0"},
                        timeout=1)

                    if ret.ok:
                        #: bytes=0-1/17)
                        content_range = ret.headers.get("Content-Range")
                        res_length = content_range.split('/')[-1]
                        break
                    else:
                        logger.warn(
                            'get piece from ip: %s port: %s error, code: %s ' %
                            (ip, port, ret.status_code))
                        count += 1
                        self.del_from_tracker(ip=ip, peer_port=port)
                except ConnectionError:
                    logger.warn(
                        'get piece from ip: %s port: %s error ConnectionError'
                        % (ip, port))
                    count += 1
                    self.del_from_tracker(ip=ip, peer_port=port)
                except Timeout:
                    logger.warn(
                        'get piece from ip: %s port: %s error Timeout' %
                        (ip, port))
                    count += 1
                    self.del_from_tracker(ip=ip, peer_port=port)
                finally:
                    if count >= peers_num:
                        logger.warn("No peers avaliable")
                        peers = self.get_peers()
                        peers_num = len(peers)
                        count = 0

            logger.info('%s is size of %s' %
                        (self.upload_res_url, sizeof_fmt_human(res_length)))

            self.piece_file = PieceFile(res_length, filepath)

            pool_work_num = 15
            pool_q_size = pool_work_num * 2
            pool = gevent.pool.Pool(pool_work_num)
            self.start_ready_upload_thread()

            if rate:
                self.download_rate = rate
            else:
                rate = self.download_rate

            if rate:
                self.token_bucket = TokenBucket(rate)

            while self.piece_file.has_unalloc():
                args_list = list()
                for peer in peers:
                    if peer not in self._peer_in_conn:
                        args_list.append((peer, None))
                [pool.apply_async(self._download_piece_thread, *args)
                    for args in args_list[:pool_q_size]]
                # update peers if peer run out
                while pool.full():
                    gevent.sleep(0.2)

                if not self.piece_file.has_empty():
                    pool.join()

                logger.debug(
                    'test get_empty_block: %s' %
                    self.piece_file.get_empty_piece())

                logger.debug('peer in connection:  %s' % self._peer_in_conn)
                if self.piece_file.has_unalloc():
                    try:
                        tv = self._get_last_get_peer_tv()
                        if tv < GET_PEER_INTERVAL:
                            gevent.sleep(GET_PEER_INTERVAL - tv)
                        g = gevent.spawn(self.get_peers)
                        peers = g.get()
                        self._record_get_peer_ts()
                    except NoPeersFound:
                        # if pool.workRequests:
                        if pool_work_num - pool.free_count() > 0:
                            # some remained piece maybe on the way
                            pool.join()
                            if self.piece_file.has_unalloc():
                                tv = self._get_last_get_peer_tv()
                                if tv > GET_PEER_INTERVAL:
                                    gevent.sleep(GET_PEER_INTERVAL - tv)
                                g = gevent.spawn(self.get_peers)
                                peers = g.get()
                                self._record_get_peer_ts()
                            else:
                                break
                        else:
                            logger.error("no worker running, and get no peers")
                            raise
                else:
                    break

            logger.info('File is complete, size: %s' %
                        self.piece_file.get_real_filesize())

        except NoPeersFound:
            if self.fallback:
                logger.info('Use fallback way to get resouce')
                try:
                    res_length = get_res_length(self.res_url)
                except ConnectionError:
                    raise OriginURLConnectError(self.res_url)
                logger.info(
                    'get resource length %s' %
                    sizeof_fmt_human(res_length))
                if not self.piece_file:
                    self.piece_file = PieceFile(res_length, filepath)

                self.start_ready_upload_thread()
                http_download_to_piecefile(
                        self.res_url, self.piece_file)
            else:
                self.deactivate()
                raise

        # self.piece_file.tofile()
        self.res_is_downloaded = True
        if callback:
            logger.info('Run callback')
            callback(**cb_kwargs)

    def res_is_ready(self):
        return self.res_is_downloaded

    def wait_for_res(self):
        while not self.res_is_downloaded:
            if not self.download_thread.is_alive():
                logger.info('download thread exit in exceptions')
                self.download_thread.join()
                # raise DownloadError('download thread exit in exceptions')
            logger.debug(
                "Waiting for resource to complete, "
                "size of %s is %s now" %
                (self.res_url, self.res_downloaded_size()))
            time.sleep(2)
        return True

    def check_download_thread(self):
        if self.download_thread.is_alive():
            return True
        else:
            self.download_thread.join()

    def _get_peer_block(self, ip, port):
        logger.debug('get peer block')
        ret = self._requests_session.get(
            "{protocol}{ip}:{port}/?{res}&pieces=all".format(
                protocol=PROTOCOL,
                ip=ip,
                port=port,
                res=urlencode({'res_url': self.upload_res_url})
                ),
            timeout=2
        )

        retjosn = ret.json()
        if retjosn['status'] == 'overload':
            raise PeerOverload
        elif retjosn['status'] == 'normal':
            return retjosn['result']

    def _get_adt_info(self):
        if self._adt_info:
            return self._adt_info

        self._adt_info = {}
        return self._adt_info

    def set_adt_info(self, adt_info):
        self._adt_info = adt_info

    def _get_strict_info(self):
        if self.strict is None:
            pass
        elif self.strict == 'site':
            return {'site': self._get_adt_info()['site']}
        else:
            raise UnknowStrict("strick = %s" % self.strict)

    def _enter_piece_thread(self, ip, port):
        self._peer_in_conn.add((ip, port))

    def _exit_piece_thread(self, ip, port):
        try:
            self._peer_in_conn.remove((ip, port))
        except KeyError:
            pass

    def get_num_peer_in_conn(self):
        return len(self._peer_in_conn)

    def _download_piece_thread(self, ip, port):
        # this function would communicate with only ontpeer
        logger.debug("in _download_piece_thread")
        # TODO: if limit rate is less than 1MB/s

        try:
            while self.is_activated():
                if not self.piece_file.has_empty():
                    logger.info(
                        'I have no piece untouched, just over this thread')
                    self._exit_piece_thread(ip, port)
                    break

                self._enter_piece_thread(ip, port)

                # get peer avaliable block
                try:
                    available_blocks = self._get_peer_block(ip, port)
                except (ConnectionError, Timeout) as e:
                    logger.info(
                        "occurr %s when get block from ip: %s, "
                        "port: %s is unavaliable, so unregister it" %
                        (e, ip, port))
                    self.del_from_tracker(ip=ip, peer_port=port)
                    self._exit_piece_thread(ip, port)
                    break
                except PeerOverload as e:
                    logger.warn(
                        'Peer ip: %s port: %s overload, let it go' %
                        (ip, port))
                    self.add_to_blacklist((ip, port), SHORT_BLACK_TIME)
                    self._exit_piece_thread(ip, port)
                    break

                logger.debug(
                    'get available block: %s from %s:%s' %
                    (available_blocks, ip, port))

                needto_downlist = self.piece_file.get_unalloc_piece(
                    available_blocks)

                logger.debug(
                    "Need to download piece %s from %s:%s" %
                    (needto_downlist, ip, port))

                # time.sleep(random.randrange(10))
                if not needto_downlist and self.piece_file.has_unalloc():
                    logger.info(
                        "add peer: %s:%s to blacklist "
                        "because that has no blocks I need"
                        % (ip, port)
                    )
                    self.add_to_blacklist((ip, port))
                    self._exit_piece_thread(ip, port)
                    break
                elif not needto_downlist:
                    self._exit_piece_thread(ip, port)
                    break

                if self.disorder:
                    needto_downlist = random.sample(
                        needto_downlist, len(needto_downlist))
                ls = ListStore(needto_downlist)

                while not ls.empty():
                    peer_in_conn = self.get_num_peer_in_conn()

                    logger.debug('peer in connection:  %s' %
                                 self._peer_in_conn)

                    # this will reduce ls
                    tobe_download_piece_idlist = \
                        self._get_pieces_once_from_ls(ls, peer_in_conn)

                    if not tobe_download_piece_idlist:
                        continue

                    try:
                        self._get_pieces_by_idlist(
                            ip, port, tobe_download_piece_idlist)
                    except Exception as e:
                        if type(e) == Timeout:
                            logger.info(
                                "Get resource from ip: %s, port: %s Time Out" %
                                (ip, port))
                            self.del_from_tracker(ip=ip, peer_port=port)
                        elif type(e) == PeerOverload:
                            self.add_to_blacklist((ip, port), SHORT_BLACK_TIME)
                            logger.warn(
                                'Peer ip: %s port: %s overload, let it go' %
                                (ip, port))
                        elif type(e) == socket.error:
                            if e.errno == socket.errno.ECONNRESET:
                                logger.warn(
                                    'Peer ip: %s port: %s reset, let it go' %
                                    (ip, port))
                            else:
                                logger.exception("Socket error occurs")
                        elif type(e) == IncompleteRead:
                            logger.error("incomplete iter_chunk")
                        elif type(e) == RateTooSlow:
                            pass
                        else:
                            logger.error(
                                "occurr %s when get piece from "
                                "ip: %s, port: %s"
                                " is unavaliable, so unregister it" %
                                (e, ip, port))
                            self.del_from_tracker(ip=ip, peer_port=port)
                        self._exit_piece_thread(ip, port)
                        return

        except Exception:
            logger.exception(
                "A Exception occurs without dealing in _download_piece_thread")

    def _get_ip(self):
        if self._ex_ip:
            return self._ex_ip

        s = socket.socket()
        if ':' in self.tracker:
            domain, port_str = self.tracker.split(':')
            tracker_tuple = (domain, int(port_str))
        else:
            tracker_tuple = (self.tracker, 80)

        try:
            s.connect(tracker_tuple)
        except socket.error as e:
            if e.errno == errno.ECONNREFUSED or e.errno == errno.ETIMEDOUT:
                raise TrackerUnavailable
            else:
                raise
        self._ex_ip = s.getsockname()[0]
        return self._ex_ip

    def upload_res(self, path=None, piece_file=None, res_name=None, rate=None):
        if res_name:
            self.upload_res_url = res_name

        if path:
            self.upload_filepath = path

        if piece_file:
            self.piece_file = piece_file

        if not self.piece_file and not path and not piece_file:
            raise ValueError(
                    "The file that is uploaded"
                    "should be download before or specify local path")

        if not rate:
            rate = self.upload_rate

        if not self.piece_file and path:
            self.piece_file = PieceFile.from_exist_file(path)

        res_mng = ResourceManager()
        res_mng.add_res(self.upload_res_url, self.piece_file)

        handler = handler_factory(res_mng, rate)
        SocketServer.TCPServer.allow_reuse_address = True

        self._is_uploading = True
        if pyinotify:
            self._start_pyinotify_when_res_ready(
                self.piece_file, self.uploader)

        self.uploader = GeventServer((self._get_ip(), 0), handler)
        t = threading.Thread(
            target=self.uploader.serve_forever,
            kwargs={'poll_interval': 0.02})
        t.start()

        ip, self.port = self.uploader.socket.getsockname()

        def func():
            interval = 5
            while self.is_activated():
                try:
                    self.add_to_tracker()
                except ConnectionError:
                    logger.warn(
                        'Tracker is down, stop registering myself to tracker')
                    self.close()
                remain = Minions.TIME_TO_TRACKER
                while remain > 0 and self.is_activated():
                    remain -= interval
                    time.sleep(interval)

        Thread(target=func).start()

    def stop_pyinotify(self):
        self._pyinotify_event = False

    def _start_pyinotify_when_res_ready(self, piece_file, uploader):
        from models import UploaderEventHandler

        def func():
            while not self.res_is_downloaded and self.is_activated():
                logger.debug(
                    'wait for resource is downloaded and start the notifier')
                time.sleep(0.2)

            self._pyinotify_event = False
            if self.is_activated():
                logger.debug('start notifier')
                wm = pyinotify.WatchManager()
                mask = pyinotify.IN_DELETE | pyinotify.IN_MODIFY |\
                    pyinotify.IN_MOVED_TO | pyinotify.IN_MOVE_SELF

                self.notifier = pyinotify.Notifier(
                        wm, UploaderEventHandler(self))
                wm.add_watch(
                    os.path.dirname(piece_file.filepath), mask, rec=False)

                self._pyinotify_event = True
                gevent.spawn(self.notifier.loop)

            while self._pyinotify_event:
                gevent.sleep(0.2)

            self.notifier.stop()

        threading.Thread(target=func).start()

    def _wait_first_block_and_upload(self):
        self._wait_uploading = True
        interval = 0.5
        while self.is_activated():
            logger.debug('wait_first_block')
            if self.piece_file.get_pieces_avail():
                logger.info('start upload')
                self.upload_res()
                self._wait_uploading = False
                break
            else:
                time.sleep(interval)

    def start_ready_upload_thread(self):
        if not self.ready_upload_thread:
            logger.debug(
                'wait the first piece downloaded, and start the uploader')
            self.ready_upload_thread = Thread(
                target=self._wait_first_block_and_upload)
            self.ready_upload_thread.start()
        else:
            logger.debug('ready upload thread is already started')

    def uploader_termiante(self):
        if self.uploader:
            self.uploader.shutdown()
            self._is_uploading = False

    def stop_upload(self):
        logger.info('delete resource: %s' % self.upload_res_url)
        # service_manager.del_res(self.upload_res_name)
        if self.uploader:
            self.del_myself_from_tracker()
            self.uploader.shutdown()
            self.uploader.socket.close()
            self._is_uploading = False

    def add_to_tracker(self):
        # for ip in self.ips:
        ip = self._get_ip()
        adt_info = self._get_adt_info()
        data = {'res': self.upload_res_url, 'ip': ip, 'port': self.port}
        if adt_info:
            data['adt_info'] = adt_info

        logger.debug(
            'Register myself as peer'
            ' which provider res: %s to tracker, '
            'ip: %s, port: %s, adt_info: %s' %
            (self.upload_res_url,
             ip, self.port,
             adt_info))

        ret = requests.post("{protocal}{host}/peer/".format(
            protocal=PROTOCOL,
            host=self.tracker,
            ),
            data=json.dumps(data),
            headers={'Content-Type': 'application/json; charset=utf-8'})
        if ret.ok:
            logger.debug('Register successfully')
        else:
            logger.error(
                'Failed to post myself to tracker, reason: %s' %
                ret.reason)

    def del_myself_from_tracker(self):
        self.del_from_tracker(self._get_ip(), self.port)

    def del_from_tracker(self, ip, peer_port):
        logger.info(
            'Unregister peer from tracker, '
            'ip:%s, port:%s, res:%s,' %
            (ip, peer_port, self.upload_res_url))
        try:
            ret = requests.delete(
                "{protocal}{host}/peer/?{res}&ip={ip}&port={peer_port}"
                .format(
                    protocal=PROTOCOL,
                    host=self.tracker,
                    res=urlencode({'res': self.upload_res_url}),
                    ip=ip,
                    peer_port=peer_port)
                )
            if ret.ok:
                logger.info('Unregister successfully')
            else:
                logger.error(
                    'Failed to unregister, reason: %s' % ret.reason)
        except ConnectionError:
            logger.error(
                'Unregister peer from tracker failed, maybe tracker is down')

    def add_to_blacklist(self, peer, time=None):
        with self._black_lock:
            self._black_peerlist.add(peer, time)

    def get_blacklist(self):
        return [peer for peer in self._black_peerlist]

    def get_file_cursor(self):
        return self.piece_file.get_cursor()

    def _get_slow_level(self):
        rate_affort = 1024 ** 2

        if self.download_rate is not None:
            slow_level = min(rate_affort, self.download_rate / 2)
        else:
            slow_level = rate_affort
        return slow_level

    def _get_pieces_by_idlist(self, ip, port, piece_idlist):
        slow_level = self._get_slow_level()

        still_empty_idlist = [piece_id for piece_id, size in piece_idlist]
        logger.debug("fetch piece list: %s at once" %
                     still_empty_idlist)
        range_str = generate_range_string(piece_idlist,
                                          self.piece_file.piece_size)

        ret = self._requests_session.get("{protocol}{ip}:{port}/?{res}".format(
                protocol=PROTOCOL,
                ip=ip,
                port=port,
                res=urlencode({'res_url': self.upload_res_url})
            ),
            stream=True,
            headers={"Range": range_str},
            timeout=2)

        if not ret.ok:
            # requests not ok
            logger.warn(
                "error occurs when peer thread getting"
                " res, reason:%s" %
                (ret.reason))

            self.del_from_tracker(ip=ip, peer_port=port)
            self.piece_file.empty_ids(still_empty_idlist)
            self._exit_piece_thread(ip, port)
            return

        rate_wait_interval = 0.1
        rate_wait_cmltime = 0
        md5sum = ret.headers.get('Content-MD5', None)

        if md5sum:
            logger.info('get Content-MD5 in headers')
            md5list = md5sum.split(',')
            hasher = md5()
        else:
            logger.warn('No Content-MD5 in headers')

        try:
            self._judge_peer_status(ret.headers)
        except PeerOverload:
            if self.get_num_peer_in_conn() > 12:
                ret.raw.close()
                ret.close()
                self.piece_file.empty_ids(still_empty_idlist)
                raise
            else:
                # We get no choice
                logger.info(
                    "Peer ip: %s port: %s is overload, "
                    "but keep using it" % (ip, port)
                )

        piece_once_num = len(piece_idlist)
        offset = 0

        all_piece_tsize = 0

        for _, size in piece_idlist:
            all_piece_tsize += size

        piece_id, size = piece_idlist[offset]

        # list contain every read block for wrote in piecefile
        buf = []

        # every single block recived length
        buflen = 0
        chunksize = 4096 * 2 * 16  # * 8

        t_recv = 0
        fetch_piece_time_start = time.time()
        try:
            for chunk in ret.iter_content(chunksize):
                recv_size = len(chunk)
                if self.token_bucket:
                    while not self.token_bucket.consume(recv_size):
                        gevent.sleep(rate_wait_interval)
                        rate_wait_cmltime += rate_wait_interval

                # That stuff after iter_comtent is cpu-relative work,
                # so switch out for doing network relative work first

                buflen += recv_size
                t_recv += recv_size

                if buflen < size:
                    # this block is incomplete

                    buf.append(chunk)
                    if md5sum:
                        hasher.update(chunk)

                else:
                    # one piece was all received
                    # one block is complete
                    overflow_size = buflen - size

                    # and data is too much for previous block
                    # leave is for next block
                    if overflow_size:
                        block = chunk[:-overflow_size]
                    else:
                        block = chunk

                    buf.append(block)
                    to_write_str = "".join(buf)
                    if md5sum:
                        hasher.update(block)
                        md5sum_get = hasher.hexdigest()
                        if md5sum_get != md5list[offset]:
                            logger.warn(
                                'md5sum is mismatch from headers: %s and '
                                'body: %s, so this peer is tainted ip: %s '
                                'port: %s' %
                                (md5list[offset], md5sum_get, ip, port))

                            logger.warn(
                                "get len: %s ,expected len: %s" %
                                (len(to_write_str), size))

                            raise PieceChecksumError(
                                md5list[offset], md5sum_get)
                            return
                        # reset hasher for next new block
                        hasher = md5()
                    else:
                        logger.warn("no MD5SUM")

                    fetch_piece_time_end = time.time()
                    fetch_time = fetch_piece_time_end - fetch_piece_time_start
                    logger.debug('get piece time: %.4f from ip: %s port: %s'
                                 % (fetch_time, ip, port))

                    if md5sum:
                        logger.info('Fill piece_id: %s md5sum %s' %
                                    (piece_id, md5sum_get))
                    else:
                        logger.info('Fill piece_id: %s' % (piece_id))

                    with elapsed_time() as pfill:
                        self.piece_file.fill(piece_id,
                                             to_write_str[0:size],
                                             md5sum_get)

                    logger.debug('fill piece time: %.4f' % pfill.elapsed_time)
                    # gevent.sleep(0)
                    del still_empty_idlist[0]

                    if size > 1024 ** 2 / 10 and\
                       self.get_num_peer_in_conn() > 12:
                        # ignore block less than 100K
                        rate_in_tranform = size / \
                            (fetch_time - rate_wait_cmltime)

                        if rate_in_tranform < slow_level:
                            # less than 1MB/s
                            logger.debug(
                                "rate_wait_cmltime: %s "
                                "size: %s "
                                "fetch time: %s"
                                % (rate_wait_cmltime, size, fetch_time)
                            )
                            logger.warn(
                                'This peer of ip: %s port: %s is '
                                'too slow to get resource, rate: %s/s' %
                                (ip, port, sizeof_fmt_human(rate_in_tranform)))

                            raise RateTooSlow(rate_in_tranform, slow_level)

                    offset += 1
                    if offset >= piece_once_num:
                        # finish
                        # should not run here
                        break
                    piece_id, size = piece_idlist[offset]
                    if overflow_size:
                        overflow_buf = chunk[-overflow_size:]
                        buf = [overflow_buf]
                        buflen = len(overflow_buf)
                    else:
                        overflow_buf = ""
                        buf = []
                        buflen = 0

                    if md5sum:
                        hasher.update(overflow_buf)

                    rate_wait_cmltime = 0
                    fetch_piece_time_start = time.time()

            if t_recv != all_piece_tsize:
                raise IncompleteRead

        except (ConnectionError, Timeout, PeerOverload,
                socket.error, IncompleteRead, RateTooSlow,
                ReadTimeout) as e:
            self.piece_file.empty_ids(still_empty_idlist)
            raise e

    def _get_pieces_once_from_ls(self, ls, peer_in_conn):
        pieces_once = 16 - peer_in_conn
        if pieces_once <= 4:
            pieces_once = 4

        # let's get pieces
        piece_idlist = []
        remain_pieces_num = pieces_once
        while True:
            pieces = ls.getlist(remain_pieces_num)
            if not pieces:
                break

            for piece in self.piece_file.get_unalloc_piece_for_fetch(
               [p_id for p_id, size in pieces]):

                piece_id, size = piece

                piece_idlist.append((piece_id, size))
                remain_pieces_num -= 1

            if remain_pieces_num == 0:
                # get the amount we need
                break

        return piece_idlist

    def _judge_peer_status(self, headers):
        peer_status = headers.get("Minions-Status", None)
        if peer_status:
            if peer_status == 'overload':
                if self.get_num_peer_in_conn() > 12:
                    raise PeerOverload


if __name__ == '__main__':
    try:
        minions_1 = Minions('AliOS5U7-x86-64.tgz', tracker='localhost:6000')
        minions_1.upload_res(path='/home/aliclone/download/os/')

        minions_2 = Minions('AliOS6U2-x86-64.tgz', tracker='localhost:6000')
        minions_2.upload_res(path='/home/aliclone/download/os/')

    except KeyboardInterrupt:
        minions_1.uploader.terminate()
        minions_2.uploader.terminate()
