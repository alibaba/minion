
import os
import sys
import time
from hashlib import md5
from utils import sizeof_fmt_human, md5sum
from threading import Thread, RLock
from peer.excepts import ChunkNotReady


class BitMap(dict):
    """
    an bitmap descript which block is fetching/empty
    True mean fetching
    False mean empty
    delete mean filled
    """
    def __init__(self, length, full=False, *args,  **kwargs):
        super(BitMap, self).__init__(*args, **kwargs)
        if not full:
            for i in range(length):
                self[i] = False

        self.size = length

    def get_fetched_block(self):
        ret_list = []
        for i in range(self.size):
            try:
                self[i]
            except KeyError:
                ret_list.append(i)
        return ret_list

    def get_fetching_block(self):
        for item in self.keys():
            if self[item] is True:
                return item

    def get_empty_block(self):
        for item in self.keys():
            if self[item] is False:
                return item

    def set_fetching_to_empty(self):
        for item in self.keys():
            self[item] = False

    def fill(self, item):
        del self[item]

    def set_empty(self, blockid):
        self[blockid] = False

    def set_fetching(self, blockid):
        self[blockid] = True

    def filter_empty_block(self, block_list):
        ret_list = []
        for blockid in block_list:
            try:
                if self[blockid] is False:
                    ret_list.append(blockid)
            except KeyError:
                pass

        return ret_list

    def filter_fetching_block(self, block_list):
        ret_list = []
        for blockid in block_list:
            if self[blockid] is True:
                ret_list.append(blockid)

        return ret_list

    def filter_exist_block(self, block_list):
        ret_list = []
        for blockid in block_list:
            try:
                self[blockid]
            except KeyError:
                ret_list.append(blockid)

        return ret_list

    filter_fetched_block = filter_exist_block


class PieceFileCursor(object):
    def __init__(self, piece_file, lock):
        self._piece_file = piece_file
        self._cursor_offset = 0
        self._start_offset = 0
        self._lock = lock
        self.md5 = md5()
        self.fileob = self._piece_file.fileob

    def write(self, buf):
        write_len = len(buf)
        self.md5.update(buf)

        with self._lock:
            self.fileob.seek(self._cursor_offset)
            self.fileob.write(buf)

        self._cursor_offset += write_len

        piece_id = self._start_offset / self._piece_file.piece_size
        if self._start_offset % self._piece_file.piece_size:
            piece_id += 1

        piece_id, size = self._piece_file.get_piece_info(piece_id)
        piece_start_offset = piece_id * self._piece_file.piece_size
        if (self._cursor_offset - piece_start_offset)\
           >= size:
            self._piece_file.fileob.flush()

            self._piece_file.filled(piece_id, md5=self.md5.hexdigest())
            self.md5 = md5()
            self._start_offset = piece_start_offset + size

    def read(self, length):
        if self.is_readable(length):
            with self._lock:
                self.fileob.seek(self._cursor_offset)
                content = self.fileob.read(length)
            self.seek(self._cursor_offset + len(content))
            return content
        else:
            raise ChunkNotReady()

    def is_readable(self, length):
        start_piece_id = self._piece_file.get_piece_id(self._cursor_offset)
        end_piece_id = self._piece_file.get_piece_id(
            self._cursor_offset + length)

        for i in range(start_piece_id, end_piece_id + 1):
            if self._piece_file.is_filled(i):
                pass
            else:
                return False
        return True

    def seek(self, offset):
        self._cursor_offset = offset
        self._start_offset = offset

    def tell(self):
        return self._cursor_offset


class PieceFile(object):
    def __init__(self, filesize, filepath, full=False, *args, **kwargs):
        self.max_len = int(filesize)
        self._lock = RLock()

        self.filepath = filepath
        self.full = full
        if full:
            self.fileob = file(filepath)
        else:
            self.fileob = file(filepath, 'w+')

        self.piece_size = 1024 ** 2

        piece_num = self.max_len / self.piece_size
        self.max_piece_id = piece_num

        last_piece_size = self.max_len % self.piece_size
        if last_piece_size >= 0:
            piece_num += 1
            self.last_piece_size = last_piece_size

        self.piece_map = BitMap(piece_num, full)

        self.piece_hash_map = dict()
        if full:
            for i in range(piece_num):
                hasher = md5()
                piece_id, piece_size = self.get_piece_info(i)
                for j in range(piece_size / 8192):
                    chunk = self.fileob.read(8192)
                    hasher.update(chunk)

                last_remain = piece_size % 8192

                if last_remain:
                    chunk = self.fileob.read(last_remain)
                    hasher.update(chunk)

                md5sum = hasher.hexdigest()
                self.piece_hash_map[piece_id] = md5sum

    def get_cursor(self, offset=0, piece_id=None):
        if piece_id:
            offset = piece_id * self.piece_size

        pfc = PieceFileCursor(self, self._lock)
        pfc.seek(offset)
        return pfc

    @classmethod
    def from_exist_file(cls, filepath):
        size = os.stat(filepath)[6]
        pf = cls(size, filepath, full=True)
        return pf

    def get_real_filesize(self, human=True):
        downloaded_size = self.max_len - len(self.piece_map.keys()) * \
            self.piece_size

        if downloaded_size < 0:
            downloaded_size = 0

        if human:
            return sizeof_fmt_human(downloaded_size)
        else:
            return downloaded_size

    def fill(self, piece_id, buf, md5=None):
        with self._lock:
            start = piece_id * self.piece_size
            self.fileob.seek(start)
            self.fileob.write(buf)
            self.fileob.flush()
            if md5:
                piece_md5 = md5
            else:
                piece_md5 = md5sum(buf)

            self.filled(piece_id, md5=piece_md5)

    def filled(self, piece_id, md5=None):
        with self._lock:
            try:
                del self.piece_map[piece_id]
            except KeyError:
                pass

        if md5:
            self.piece_hash_map[piece_id] = md5

    def is_filled(self, piece_id):
        try:
            self.piece_map[piece_id]
        except KeyError:
            return True
        return False

    def get_piece_md5(self, piece_id):
        return self.piece_hash_map[piece_id]

    def empty(self, piece_id):
        with self._lock:
            self.piece_map.set_empty(piece_id)

    def empty_ids(self, piece_ids):
        with self._lock:
            for piece_id in piece_ids:
                self.piece_map.set_empty(piece_id)

    def get_unalloc_piece(self, piece_idlist=None):
        # It is thread safe
        with self._lock:
            if piece_idlist:
                ret = []
                empty_piece_idlist = self.piece_map.filter_empty_block(
                    piece_idlist)
                for piece_id in empty_piece_idlist:
                    ret.append(self.get_piece_info(piece_id))
                return ret

            piece_id = self.piece_map.get_empty_block()
            if piece_id == self.max_piece_id and self.last_piece_size > 0:
                return piece_id, self.last_piece_size
            elif piece_id is None:
                return None, None
            else:
                return piece_id, self.piece_size

    def get_unalloc_piece_for_fetch(self, piece_idlist=None):
        with self._lock:
            ret = self.get_unalloc_piece(piece_idlist)
            if piece_idlist:
                for piece_id, size in ret:
                    self.piece_map.set_fetching(piece_id)
            else:
                piece_id, size = ret
                if piece_id is not None:
                    self.piece_map.set_fetching(piece_id)
            return ret

    def get_unalloc_piece_by_id(self, piece_id):
        with self._lock:
            empty_piece_idlist = self.piece_map.filter_empty_block([piece_id])
            try:
                piece_id = empty_piece_idlist[0]
                return self.get_piece_info(piece_id)
            except IndexError:
                return None, None

    def get_unalloc_piece_by_id_for_fetch(self, piece_id):
        with self._lock:
            piece_id, size = self.get_unalloc_piece_by_id(piece_id)
            if piece_id is not None:
                self.piece_map.set_fetching(piece_id)

            return piece_id, size

    def get_piece_info(self, piece_id):
        if piece_id == self.max_piece_id and self.last_piece_size > 0:
            return piece_id, self.last_piece_size
        else:
            return piece_id, self.piece_size

    def get_piece_id(self, offset):
        piece_id = offset / self.piece_size
        if offset % self.piece_size:
            piece_id += 1

        return piece_id

    def put_to_queue(self):
        for i in self.piece_map.keys():
            self.queue.put(i)

    def get_empty_piece(self):
        return self.piece_map.get_empty_block()

    def has_empty(self):
        # empty means no fetching or filled
        with self._lock:
            if self.piece_map.get_empty_block() is not None:
                return True
            else:
                return False

    def has_unalloc(self):
        # it is not thread safe, just be careful
        with self._lock:
            return len(self.piece_map.keys()) > 0

    def get_pieces_avail(self, piece_idlist=None):
        if piece_idlist:
            return self.piece_map.filter_fetched_block(piece_idlist)
        else:
            return self.piece_map.get_fetched_block()

    def __len__(self):
        return self.max_len


class ExpireWrapper(object):
    def __init__(self, obj, expire_time=2):
        self._obj = obj
        self.expire_time = time.time() + expire_time
        self._is_expired = False

    def is_expired(self):
        if not self._is_expired:
            self._is_expired = (time.time() - self.expire_time) > 0
        return self._is_expired

    def set_expire_time(self, expire_time=2):
        self.expire_time = time.time() + expire_time

    def get_obj(self):
        return self._obj

    def __str__(self):
        return str(self._obj)

    def __repr__(self):
        return repr(self._obj)


class ExpireStorage(object):
    def __init__(self, expire_time=2):
        self._set = set()
        self._expire_time = expire_time

    def add(self, obj, time=None):
        if time:
            expire_time = time
        else:
            expire_time = self._expire_time

        if obj in self:
            for ew_obj in self._set:
                if ew_obj.is_expired():
                    continue
                else:
                    if ew_obj.get_obj() is obj:
                        ew_obj.set_expire_time(expire_time)
        else:
            ew_obj = ExpireWrapper(obj, self._expire_time)
            self._set.add(ew_obj)

    def __iter__(self):
        for ew_obj in self._set.copy():
            if ew_obj.is_expired():
                continue
            else:
                yield ew_obj.get_obj()

    def __str__(self):
        return str(self._set)

    def _repr__(self):
        return repr(self._set)


class ExcThread(Thread):
    def __init__(self, *args, **kwargs):
        super(ExcThread, self).__init__(*args, **kwargs)
        self._exc_info = None

    def run(self):
        try:
            super(ExcThread, self).run()
        except:
            self._exc_info = sys.exc_info()

    def join(self, timeout=0):
        super(ExcThread, self).join(timeout)
        if self._exc_info:
            raise self._exc_info[0], self._exc_info[1], self._exc_info[2]


try:
    import pyinotify

    class UploaderEventHandler(pyinotify.ProcessEvent):
        def __init__(self, minion):
            super(UploaderEventHandler, self).__init__()
            self.minion = minion
            self.filepath = os.path.abspath(minion.piece_file.filepath)

        def is_my_filepath(self, filepath):
            return self.filepath == filepath

        def stop_upload(self):
            self.minion.logger.info('File %s was modified, stop uploading' %
                                    self.filepath)
            self.minion.stop_pyinotify()
            self.minion.stop_upload()

        def process_IN_DELETE(self, event):
            if self.is_my_filepath(event.pathname):
                self.minion.logger.info('DELETE')
                self.stop_upload()

        def process_IN_MODIFY(self, event):
            if self.is_my_filepath(event.pathname):
                self.minion.logger.info('MODIFY')
                self.stop_upload()

        def process_IN_MOVED_TO(self, event):
            if self.is_my_filepath(event.pathname):
                self.minion.logger.info('MOVED TO')
                self.stop_upload()

        def process_IN_MOVE_SELF(self, event):
            if self.is_my_filepath(event.pathname):
                self.minion.logger.info('MOVE self')
                self.stop_upload()

except ImportError:
    pyinotify = None
