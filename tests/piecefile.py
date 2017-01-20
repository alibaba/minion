import time
import unittest
import threading
from hashlib import md5

from memory_profiler import profile

from peer.models import PieceFile
from peer.utils import hash_file, mprofile
from peer.excepts import ChunkNotReady

FILE_SIZE = 755 * 1024 * 1024 + 702
FILE_PATH = '/tmp/piecefile'


def iter_length(length, size):
    left = length
    while True:
        if left > size:
            left -= size
            yield size
        else:
            yield left
            break


class PieceFileTest(unittest.TestCase):
    # @profile
    def setUp(self):
        # self.buf = [(i % 10)+49 for i in range(1024)]
        self.buf = bytearray("0" * 1024 ** 2)
        self.buf[3] = "1"
        self.buf[30] = "1"
        self.piece_file = PieceFile(FILE_SIZE, FILE_PATH)

    @profile
    def test_all(self):
        a = list()
        for i in range(5):
            a.append(threading.Thread(target=self.func))
        start = time.time()

        for i in a:
            i.start()

        for i in a:
            i.join()

        elasp = time.time() - start

        # print self.piece_file
        print 'use %s(s)' % elasp
        self._verify_file()

    def func(self):
        while True:
            piece_id, size = self.piece_file.get_unalloc_piece_for_fetch()
            if piece_id is not None:
                buf = self._mock_get_data()
                self.piece_file.fill(piece_id, buf[0:size])
            else:
                break

    def func1(self):
        while True:
            piece_id, size = self.piece_file.get_unalloc_piece_for_fetch()
            buf = self._mock_get_data()
            if piece_id is not None:
                cursor = self.piece_file.get_cursor(piece_id=piece_id)
                cursor.write(buf[0:size])
            else:
                break

    def random_write(self, piece_file):
        buf = self._mock_get_data()
        buflen = len(buf)
        while True:
            piece_id, size = piece_file.get_unalloc_piece_for_fetch()
            if piece_id is not None:
                cursor = piece_file.get_cursor(piece_id=piece_id)
                for i in iter_length(size, buflen):
                    cursor.write(buf[0:i])
            else:
                break

    @profile
    def test_cursor_write(self):
        a = list()
        for i in range(5):
            a.append(threading.Thread(target=self.func1))
        start = time.time()

        for i in a:
            i.start()

        for i in a:
            i.join()

        elasp = time.time() - start

        # print self.piece_file
        print 'use %s(s)' % elasp
        self._verify_file()

    @mprofile
    def test_cursor_order_write(self):
        cursor = self.piece_file.get_cursor()
        while True:
            piece_id, size = self.piece_file.get_unalloc_piece_for_fetch()
            if piece_id is not None:
                offset = 0
                buf = self._mock_get_data()
                for i in iter_length(size, 8196):
                    cursor.write(buf[offset:offset + i])
                    offset += i
            else:
                break

        self._verify_file()

    def test_cursor_read_fallback(self):
        pass

    # @profile
    def test_cursor_order_read(self):
        piece_file = PieceFile(FILE_SIZE, FILE_PATH)
        cursor = piece_file.get_cursor()
        a = list()

        for i in range(10):
            a.append(
                threading.Thread(
                    target=self.random_write, args=(piece_file,)))

        for i in a:
            i.start()

        md5sum = md5()
        buf = self._mock_get_data()
        chunksize = len(buf)
        while True:
            if cursor.is_readable(chunksize):
                try:
                    content = cursor.read(chunksize)
                    if not content:
                        break
                    self.assertEqual(content, buf[0:len(content)])
                    md5sum.update(content)
                except ChunkNotReady:
                    break
            else:
                time.sleep(0.2)

        self.assertEqual(
            md5sum.hexdigest(), '9e503d2e6a09f60dc8b28e8330b686e0')

    def _mock_get_data(self):
        return self.buf

    def _verify_file(self):
        self.assertEqual(len(self.piece_file), FILE_SIZE)
        hash_ret = hash_file(FILE_PATH, 'md5')
        print 'md5sum: %s' % hash_ret
        self.assertEqual(hash_ret, '9e503d2e6a09f60dc8b28e8330b686e0')

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
