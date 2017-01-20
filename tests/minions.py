
import os
import time
import unittest
from threading import Thread
from multiprocessing import Process
from peer.libs.mrequests import requests
from peer.client import Minions
from peer.excepts import NoPeersFound
from peer.utils import logging_config


logging_config("WARN")

TRACKER = 'localhost:5000'

FILE_SIZE = 705 * 1024 * 1024 + 702


class P4PTest(unittest.TestCase):
    def setUp(self):
        self.res_file = 'res_file.tgz'
        # create test file
        if not os.path.exists(self.res_file):
            with file(self.res_file, 'w+') as f:
                [f.write("0" * 1024) for i in xrange(FILE_SIZE/1024)]
                f.write("0" * (FILE_SIZE % 1024))

        from tracker import runserver
        self.server_process = runserver('localhost', 5000)

        time.sleep(0.2)

        self.tmp_res_fpath = '/tmp/minions_1.tgz'

    def test_no_peers(self):

        with self.assertRaises(NoPeersFound):
            minion_1 = Minions('res_file.tgz',
                               '/tmp/minions_1.tgz',
                               tracker=TRACKER)
            minion_1.download_res()

    def _get_uploader_process(self):
        def _func():
            minion_uploader = Minions('res_file.tgz', tracker=TRACKER)
            minion_uploader.upload_res('./res_file.tgz')
            while True:
                time.sleep(1000)

        p = Process(target=_func)
        return p

    def test_P4P_ignore_url_param(self):
        minion_uploader = Minions('res_file.tgz', tracker=TRACKER)
        minion_uploader.upload_res('./res_file.tgz')

        time.sleep(1)

        requests.get('http://localhost:5000/peer/?res=res_file.tgz')

        minions = []
        try:
            for i in range(3):
                minion = Minions(
                        'res_file.tgz?a=%s&b=%s' % (i, i),
                        '/tmp/res_file%s.tgz' % i, tracker=TRACKER,
                        upload_res_url="res_file.tgz")
                minion.download_res(rate=20 * 1024 ** 2, thread=True)
                time.sleep(0.1)
                minions.append(minion)
            time.sleep(2)

            for m in minions:
                m.wait_for_res()

        except Exception:
            import traceback
            traceback.print_exc()
            raise
        finally:
            for m in minions:
                m.close()
            minion_uploader.close()

    # @gprofile
    def test_P4P(self):
        minion_uploader = Minions(
                'res_file.tgz', upload_rate=150 * 1024 ** 2, tracker=TRACKER)
        minion_uploader.upload_res('./res_file.tgz')

        time.sleep(1)

        requests.get('http://localhost:5000/peer/?res=res_file.tgz')

        minions = []
        try:
            for i in range(1):
                minion = Minions(
                    'res_file.tgz',
                    '/tmp/res_file%s.tgz' % i,
                    tracker=TRACKER)
                minions.append(minion)
                minion.download_res(rate=1500 * 1024 ** 2)
                time.sleep(0.1)
            time.sleep(2)

            for m in minions:
                m.wait_for_res()

        except Exception:
            import traceback
            traceback.print_exc()
            raise
        finally:
            for m in minions:
                m.close()
            minion_uploader.close()

    def test_fallback(self):
        import BaseHTTPServer
        from SimpleHTTPServer import SimpleHTTPRequestHandler

        server_address = ('', 8000)
        httpd = BaseHTTPServer.HTTPServer(
                server_address,
                SimpleHTTPRequestHandler)
        t = Thread(target=httpd.serve_forever)
        t.start()

        try:
            minion_1 = Minions(
                    'http://localhost:8000/res_file.tgz',
                    '/tmp/res_file.tgz',
                    fallback=True,
                    tracker=TRACKER)

            minion_1.download_res()
            time.sleep(1)
            minion_2 = Minions(
                'http://localhost:8000/res_file.tgz',
                '/tmp/res_file1.tgz', tracker=TRACKER)
            minion_2.download_res(rate=20 * 1024 ** 2)
        finally:
            minion_1.close()
            minion_2.close()
            httpd.shutdown()

    def test_get_peer_strict(self):
        minion_uploader = Minions('res_file.tgz', tracker=TRACKER)
        minion_uploader.set_adt_info({'site': 'mysite1'})
        minion_uploader.upload_res('./res_file.tgz')

        minion_2 = None
        try:
            with self.assertRaises(NoPeersFound):
                minion_1 = Minions(
                        'res_file.tgz',
                        '/tmp/res_file1.tgz',
                        strict='site',
                        tracker=TRACKER)
                minion_1.set_adt_info({'site': 'mysite2'})
                minion_1.download_res()

            minion_2 = Minions(
                    'res_file.tgz',
                    '/tmp/res_file2.tgz',
                    strict='site',
                    tracker=TRACKER)
            minion_2.set_adt_info({'site': 'mysite1'})
            minion_2.download_res()
        finally:
            minion_uploader.close()
            minion_1.close()
            if minion_2:
                minion_2.close()

    def test_runout_peer(self):
        minion_uploader = Minions('res_file.tgz', tracker=TRACKER)
        minion_uploader.upload_res('./res_file.tgz')

        time.sleep(1)

        requests.get('http://localhost:5000/peer/?res=res_file.tgz')

        with self.assertRaises(NoPeersFound):
            try:
                minion_1 = Minions(
                        'res_file.tgz', '/tmp/res_file.tgz',
                        tracker=TRACKER)
                minion_1.download_res(rate=10 * 1024 ** 2, thread=True)
                time.sleep(2)
                minion_uploader.close()
                minion_1.wait_for_res()
            except Exception as e:
                import traceback
                traceback.print_exc()
                raise e
            finally:
                minion_1.close()
                minion_uploader.close()

    def tearDown(self):
        self.server_process.terminate()


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(P4PTest("test_no_peers"))
    suite.addTest(P4PTest("test_P4P"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
