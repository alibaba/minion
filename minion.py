#!/usr/bin/env python


import sys
import os
import time
import errno
import hashlib
import argparse
import urlparse
import logging
import traceback
import tempfile
import select
import socket
import mmap
from multiprocessing import Process, Queue

from peer.client import Minions
from peer.excepts import ChunkNotReady
from peer.utils import logging_config, strip_url_qp

__VERSION__ = '0.5.6'
__AUTHORS__ = [
    "linxiulei@gmail.com"
]

logging_config("INFO")

logger = logging.getLogger('CLI')

STDOUT_SIZE = 8192 * 16


class RETCODE(object):
    success = 0
    checksum_missmatch = 4
    tracker_unavaliable = 5
    dns_fail = 6
    origin_fail = 7


class ChecksumMissMatch(Exception):
    pass


def hash_file(filepath, hashtype):
    BLOCKSIZE = 65536
    hasher = getattr(hashlib, hashtype)()
    f = file(filepath)
    buf = f.read(BLOCKSIZE)
    while len(buf) > 0:
        hasher.update(buf)
        buf = f.read(BLOCKSIZE)
    f.close()
    return hasher.hexdigest()


def cb_verify_hash(filepath, hashtype, hashsum):
    real_hashsum = hash_file(filepath, hashtype)
    if real_hashsum != hashsum:
        raise ChecksumMissMatch("actually %s, should be %s" %
                                (real_hashsum, hashsum))


class StoreUnitAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        units = {
            "K": 1024,
            "M": 1024 ** 2,
            "G": 1024 ** 3,
            "T": 1024 ** 4,
        }
        for u in units.keys():
            if values.endswith(u):
                setattr(namespace, self.dest, int(values[:-1]) * units[u])


class HashAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        try:
            hashtype, hashsum = value.split(":")
            setattr(namespace, 'hash', value)
            setattr(namespace, 'hash_type', hashtype)
            setattr(namespace, 'hash_sum', hashsum)
        except ValueError:
            raise argparse.ArgumentError(self, "format like HASHTYPE:HASHSUM")

        try:
            getattr(hashlib, hashtype)
        except AttributeError:
            raise argparse.ArgumentError(self, "unknow hashtype: %s" %
                                         hashtype)


def parse_args():
    parser = argparse.ArgumentParser("P4P tool cli")
    subparsers = parser.add_subparsers(help="sub-command help")

    parser_get = subparsers.add_parser("get", help="get data")
    parser_get.add_argument("url", action="store")
    parser_get.add_argument(
        "--tracker", action="store", metavar="HOST",
        default="p2p-tracker.alibaba-inc.com", dest="tracker",
        help='specify tracker server')
    parser_get.add_argument(
        "--dest-path", metavar="localpath",
        action="store", dest="dest_path", help="path download, can be dir")
    parser_get.add_argument(
        "--download-rate", action=StoreUnitAction,
        metavar="int_with_unit", dest="download_rate",
        help='network rate limit which can with unit. e.g. 10M')
    parser_get.add_argument(
        "--upload-rate", action=StoreUnitAction,
        metavar='int_with_unit', dest="upload_rate",
        help='network rate limit which can with unit, e.g. 10M')
    parser_get.add_argument(
        "--upload-time", action="store", type=int,
        dest="upload_time", default=10,
        help="upload time after download completely")
    parser_get.add_argument(
        "--fallback", action="store_true", default=False,
        dest="fallback",
        help="download from origin source when no peer available")
    parser_get.add_argument(
        "--callback", type=str, choices=['delete'],
        dest="callback",
        help="method invoke when work all over")
    parser_get.add_argument(
        "--hash", action=HashAction, dest="hash",
        metavar="HASHTYPE:HASHSUM",
        help="specify hash type and hash sum to verify downloaded file")
    parser_get.add_argument(
        "--ignore-qp", nargs='*', dest="ignore_qp",
        help="ignore specified query param for resource uploading")
    parser_get.add_argument(
        "--data-stdout", dest="data_stdout",
        action="store_true", default=False,
        help="redirect downloading file to stdout")
    parser_get.add_argument(
        "--verbose", action="store", type=int,
        default=0, dest="verbose", help="you should know what it mean")
    parser_get.add_argument(
        "--logfile", action="store", dest="logfile",
        help="specify logfile path")

    return parser.parse_args()


class Daemon(object):
    def __init__(self, args, q, sock, mm=None):
        self.args = args
        self.q = q
        self.sock = sock
        self.mm = mm

    def run(self):
        try:
            minion = get_minion(self.args)
            if self.args.data_stdout:
                minion.download_res(thread=True)

                # wait download initial
                while minion.check_download_thread():
                    if minion.res_downloaded_size() > 0:
                        break
                    else:
                        time.sleep(0.2)

                cursor = minion.get_file_cursor()

                while True:
                    minion.check_download_thread()
                    try:
                        a = cursor.read(STDOUT_SIZE)
                        if a:
                            size = len(a)
                            self.mm[:size] = a
                            self.sock.send(str(size))
                            self.sock.recv(10)
                        else:
                            break
                    except ChunkNotReady as e:
                        time.sleep(0.2)

            elif self.args.hash:
                minion.download_res(
                    callback=cb_verify_hash,
                    cb_kwargs={
                        'filepath': self.args.dest_path,
                        'hashtype': self.args.hash_type,
                        'hashsum': self.args.hash_sum,
                    })
            else:
                minion.download_res()
            self.q.put((0, True))
        except Exception:
            except_type, except_class, tb = sys.exc_info()
            self.q.put(
                        (1,
                            (
                                (except_type,
                                 except_class,
                                 traceback.extract_tb(tb))
                            )
                         )
                      )
            return

        # minion.upload_res(path=args.dest_path)
        poll_time = 10
        remain_upload_time = self.args.upload_time
        while minion.is_uploading() or minion.is_wait_uploading():
            remain_upload_time -= poll_time
            if remain_upload_time > 0:
                time.sleep(poll_time)
            else:
                # time is up
                time.sleep(remain_upload_time + poll_time)
                break

        minion.close()
        if self.args.callback == 'delete':
            try:
                os.remove(self.args.dest_path)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    logger.warn('file is already removed')
                else:
                    raise

    def start(self):
        def child():
            if self.args.logfile:
                os.close(sys.stdout.fileno())
                os.close(sys.stderr.fileno())

            Process(target=self.run).start()
            os.kill(os.getpid(), 15)

        Process(target=child).start()


def get_minion(args):
    if args.data_stdout:
        disorder = False
    else:
        disorder = True

    if args.ignore_qp:
        upload_res_url = strip_url_qp(args.url, args.ignore_qp)
        minion = Minions(
            args.url,
            download_dest=args.dest_path,
            download_rate=args.download_rate,
            upload_rate=args.upload_rate,
            upload_res_url=upload_res_url,
            fallback=args.fallback,
            tracker=args.tracker,
            disorder=disorder)
    else:
        minion = Minions(
            args.url,
            download_dest=args.dest_path,
            download_rate=args.download_rate,
            upload_rate=args.upload_rate,
            fallback=args.fallback,
            tracker=args.tracker,
            disorder=disorder)
    return minion


def get_filename_from_url(url):
    return urlparse.urlparse(url).path.split('/')[-1]


def pre_args(args):
    if args.data_stdout:
        if args.hash:
            errmsg = "--hash not support in --data-stdout"
            raise ValueError(errmsg)

        if not args.dest_path:
            args.dest_path = tempfile.mktemp(prefix="/var/tmp/")
            args.callback = "delete"

        if args.verbose > 0 and not args.logfile:
            logger.warn("Should not use verbose > 0 when --data-stdout")

    if not args.dest_path:
        args.dest_path = "."

    if os.path.isdir(args.dest_path):
        filename = get_filename_from_url(args.url)
        args.dest_path = os.path.join(args.dest_path, filename)


if __name__ == '__main__':
    retcode = 0
    args = parse_args()
    pre_args(args)
    if args.verbose == 0:
        logging_config("ERROR", args.logfile)
    elif args.verbose == 1:
        logging_config("INFO", args.logfile)
    elif args.verbose == 2:
        logging_config("DEBUG", args.logfile)

    msg_q = Queue()
    try:
        psock, csock = socket.socketpair()
        if args.data_stdout:
            mm = mmap.mmap(-1, STDOUT_SIZE)
        else:
            mm = None
        d = Daemon(args, msg_q, csock, mm)
        d.start()
        # wait donwload finish
        while msg_q.empty():
            rlist, _, _ = select.select([psock], [], [], 1)
            if rlist:
                size = int(psock.recv(10))
                sys.stdout.write(mm[:size])
                psock.send("1")
        ret = msg_q.get()

        if ret[0] == 0 and ret[1] is True:
            logger.info('downloaded, upload %s second(s) background' %
                        args.upload_time)
            retcode = 0
        elif ret[0] == 1:
            logger.error("Traceback (most recent call last):")
            exc_type, exc_obj, exc_trace = ret[1]
            logger.error("".join(traceback.format_list(exc_trace))
                         + exc_type.__name__ + ": " + str(exc_obj))
            retcode = 1
            if exc_type.__name__ == 'ChecksumMissMatch':
                logger.error(exc_obj.message)
                retcode = RETCODE.checksum_missmatch
            elif exc_type.__name__ == 'TrackerUnavailable':
                logger.error("Tracker Server: %s Unavaliable" % args.tracker)
                retcode = RETCODE.tracker_unavaliable
            elif exc_type.__name__ == 'gaierror':
                logger.error("Domain name resolve failed, check DNS")
                retcode = RETCODE.dns_fail
            elif exc_type.__name__ == 'OriginURLConnectError':
                logger.error("Get data from origin url fail")
                retcode = RETCODE.origin_fail

        sys.exit(retcode)
    except Exception as e:
        raise
