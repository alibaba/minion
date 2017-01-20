
import cgi
import time
import socket
import hashlib
import logging.config
import urlparse
import requests
import pstats
import StringIO
import cProfile
from threading import Lock
from threading import Thread
from errno import EAGAIN
from sendfile import sendfile as original_sendfile
from gevent.socket import wait_write

try:
    import pyinotify
except ImportError:
    pyinotify = None


def _get_attach_filename(content_disposition):
    # content_disposition = attachment; filename=clonescripts.tgz"
    if content_disposition:
        content_type, params = cgi.parse_header(
                                   content_disposition)
        return params['filename']
    return ""


class ListStore(object):
    def __init__(self, list_obj):
        self._list = list_obj
        self._offset = 0
        self._length = len(list_obj)

    def getlist(self, num=1):
        new_list = self._list[self._offset:self._offset + num]
        self._offset += num
        return new_list

    def empty(self):
        return self._offset >= self._length

    def get(self):
        obj = self._list[self._offset]
        self._offset += 1
        return obj


def get_res_length(url):
    r = requests.get(url, stream=True)
    r.close()
    if not r.ok:
        r.raise_for_status()

    length_str = r.headers['Content-Length']

    return int(length_str)


def http_download_to_piecefile(url, piecefile, thread=False):
    def func(url, piecefile):
        chunksize = 8192

        r = requests.get(url, stream=True)

        if not r.ok:
            r.raise_for_status()

        cursor = piecefile.get_cursor()
        t_size = 0
        for chunk in r.iter_content(chunksize):
            t_size += len(chunk)
            if chunk:
                cursor.write(chunk)
            else:
                break

    if thread:
        t = Thread(target=func, kwargs={'url': url, 'piecefile': piecefile})
        t.start()
    else:
        func(url, piecefile)


def sizeof_fmt_human(num):
    if type(num) == str:
        num = float(num)
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def hash_file(filepath, hashtype):
    hash_obj = getattr(hashlib, hashtype)
    myhash = hash_obj()
    with file(filepath) as f:
        myslice = f.read(8196)
        while myslice:
            myhash.update(myslice)
            myslice = f.read(8196)

    return myhash.hexdigest()


def logging_config(level='INFO', logfile=None):
    if level == "DEBUG":
        format_string = '%(asctime)s %(levelname)s %(name)s'\
                        ' %(thread)d %(message)s'
    else:
        format_string = '%(asctime)s %(levelname)s %(name)s'\
                        ' %(message)s'

    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': format_string,
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'default'
                }
        },
        'root': {
            'level': level,
            'handlers': []
        },
        'loggers': {
            'requests': {
                'level': 'ERROR'
            },
        }
    }

    if logfile:
        LOGGING['handlers']['file'] = {
                'class': 'logging.FileHandler',
                'filename': logfile,
                'level': level,
                'mode': 'w',
                'formatter': 'default'
                }

        LOGGING['root']['handlers'].append('file')
    else:
        LOGGING['root']['handlers'].append('console')

    logging.config.dictConfig(LOGGING)


class TokenBucket(object):
    """An implementation of the token bucket algorithm.

    >>> bucket = TokenBucket(80, 0.5)
    >>> print bucket.consume(10)
    True
    >>> print bucket.consume(90)
    False
    """

    stat_interval = 0.4

    def __init__(self, fill_rate):
        """
        tokens is the total tokens in the bucket. fill_rate is the
        rate in tokens/second that the bucket will be refilled.
        """
        self._tokens = 0
        self._last_consumed_tokens = 0
        self._consumed_tokens = 0
        self.fill_rate = float(fill_rate)
        self.timestamp = time.time()
        self._rate_ts = time.time()
        self.rate = 0
        self._lock = Lock()

        self._run_stat_thread = False

    def get_rate_usage(self):
        return self.rate / self.fill_rate

    def start_stat_thread(self):
        self._run_stat_thread = True

        def func():
            while self._run_stat_thread:
                self.stat_rate()
                time.sleep(self.stat_interval)

        Thread(target=func).start()

    def stop_stat_thread(self):
        self._run_stat_thread = False

    def consume(self, tokens):
        """Consume tokens from the bucket. Returns True if there were
        sufficient tokens otherwise False."""
        if tokens <= self.tokens:
            self._tokens -= tokens
            self._consumed_tokens += tokens
        else:
            return False
        return True

    def get_rate(self):
        self.stat_rate()
        return self.rate

    def stat_rate(self):
        now = time.time()
        with self._lock:
            delta_seconds = now - self._rate_ts
            if delta_seconds > self.stat_interval:
                self.rate = (
                                self._consumed_tokens -
                                self._last_consumed_tokens
                            ) / delta_seconds
                self._rate_ts = now
                self._last_consumed_tokens = self._consumed_tokens

    def get_tokens(self):
        now = time.time()
        delta_seconds = min(now - self.timestamp, 2)
        delta = self.fill_rate * delta_seconds
        self._tokens += delta
        self.timestamp = now
        return self._tokens

    tokens = property(get_tokens)


class elapsed_time(object):
    def __init__(self):
        self.elapsed_time = 0

    def __enter__(self):
        self.begin = time.time()
        return self

    def __exit__(self, type, value, traceback):
        self.elapsed_time = time.time() - self.begin


def md5sum(buf):
    m = hashlib.md5()
    m.update(buf)
    return m.hexdigest()


def join_qsl(qsl):
    ret_qs = []
    for k, v in qsl:
        if v:
            ret_qs.append("%s=%s" % (k, v))
        else:
            ret_qs.append("%s" % k)
    return '&'.join(ret_qs)


def strip_url_qp(url, qp):
    pr = urlparse.urlparse(url)
    qsl = urlparse.parse_qsl(pr.query, True)
    new_qsl = [(k, v) for k, v in qsl if k not in qp]
    url = urlparse.urlunparse(
        (
            pr.scheme,
            pr.netloc,
            pr.path,
            pr.params,
            join_qsl(new_qsl),
            pr.fragment)
        )
    return url


def generate_range_string(piece_info, piece_size):
    range_str = "bytes="
    value_list = []
    for piece_id, size in piece_info:
        start = piece_id * piece_size
        end = start + size - 1
        value_list.append("%s-%s" % (start, end))
    range_str += ",".join(value_list)
    return range_str


def is_gaierror(e):
    if getattr(e, "args", None):
        try:
            if type(e.args[0][1]) == socket.gaierror:
                return True
        except KeyError:
            pass
    return False


def mprofile(func):
    def func_wrapper(*args, **kwargs):
        pr = cProfile.Profile()
        pr.enable()
        ret = func(*args, **kwargs)
        # ... do something ...
        pr.disable()
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print s.getvalue()

        return ret
    return func_wrapper


try:
    import GreenletProfiler

except ImportError:
    GreenletProfiler = None


def gprofile(func):
    def func_wrapper(*args, **kwargs):
        if GreenletProfiler:
            GreenletProfiler.set_clock_type('wall')
            GreenletProfiler.start()
            ret = func(*args, **kwargs)
            GreenletProfiler.stop()
            stats = GreenletProfiler.get_func_stats()
            stats.print_all()
            stats.save('profile.callgrind', type='callgrind')
            return ret
        return func(*args, **kwargs)
    return func_wrapper


"""An example how to use sendfile[1] with gevent.
[1] http://pypi.python.org/pypi/py-sendfile/
"""
# pylint:disable=import-error


def gevent_sendfile(out_fd, in_fd, offset, count):
    total_sent = 0
    sent = 0
    while total_sent < count:
        try:
            sent = original_sendfile(
                out_fd, in_fd, offset + total_sent, count - total_sent)
            total_sent += sent
        except OSError as ex:
            if ex.args[0] == EAGAIN:
                sent = 0
                # TODO: there is performance problem maybe
                wait_write(out_fd)
            else:
                raise
    return total_sent


if __name__ == '__main__':
    from time import sleep
    bucket = TokenBucket(15)
    bucket.start_stat_thread()
    print "tokens =", bucket.tokens
    print "consume(10) =", bucket.consume(10)
    sleep(0.3)
    print "consume(10) =", bucket.consume(10)
    print "tokens =", bucket.tokens
    sleep(1)
    print "tokens =", bucket.tokens
    print "consume(90) =", bucket.consume(90)
    print "tokens =", bucket.tokens
    print 'consume speed = %s b/s' % bucket.get_rate()
    print 'sleep 1s'
    sleep(1)
    print 'consume speed = %s b/s' % bucket.get_rate()
    print 'get rate usage = %s' % bucket.get_rate_usage()
    bucket.stop_stat_thread()

    with elapsed_time() as p:
        time.sleep(1.123)

    print p.elapsed_time
