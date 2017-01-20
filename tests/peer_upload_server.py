
import os
import unittest
import requests
import threading
from urllib import urlencode
from peer.models import PieceFile
from peer.server import ResourceManager, handler_factory, GeventServer

FILE_SIZE = 705 * 1024 * 1024 + 702


class UploadServerTest(unittest.TestCase):
    def setUp(self):
        self.res_file = 'res_file.tgz'

        if not os.path.exists(self.res_file):
            with file(self.res_file, 'w+') as f:
                [f.write("0" * 1024) for i in xrange(FILE_SIZE/1024)]
                f.write("0" * (FILE_SIZE % 1024))

        with file(self.res_file) as f:
            self.begin_slice = f.read(2)

    def test_server(self):
        res_mng = ResourceManager()
        res_url = 'http://testserver.localhost/bigfile'
        res_mng.add_res(res_url, PieceFile.from_exist_file('res_file.tgz'))
        handler = handler_factory(res_mng, None)
        uploader = GeventServer(("0.0.0.0", 0), handler)

        t = threading.Thread(
            target=uploader.serve_forever,
            kwargs={'poll_interval': 0.02})
        t.start()

        ip, port = uploader.socket.getsockname()
        print 'server in %s:%s' % (ip, port)
        try:
            ret = requests.get(
                'http://localhost:%s?%s' %
                (port, urlencode({'res_url': res_url})),
                headers={"Range": "bytes=0-1"}
            )
            self.assertEqual(ret.content, self.begin_slice)

            ret = requests.get(
                'http://localhost:%s?%s&%s' %
                (
                    port, urlencode({'res_url': res_url}),
                    "pieces=1,2,3,400"
                ),
            )
            self.assertEqual(
                ret.content,
                '{"status": "normal", "result": ["1", "2", "3", "400"]}')
        finally:
            uploader.shutdown()


if __name__ == '__main__':
    unittest.main()
