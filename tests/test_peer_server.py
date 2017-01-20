import time
import unittest
import socket

import gevent
import requests

from peer.client import Minions
from peer.utils import logging_config

logging_config("ERROR")

host = "localhost"
port = 5001
peer_url = "http://%s:%s/peer/" % (host, port)
TRACKER = 'localhost:5001'


class PeerServerTest(unittest.TestCase):
    def setUp(self):
        from tracker import runserver
        self.server_process = runserver(host, port)
        time.sleep(1)

    def test_keepalive_timeout(self):
        minion_uploader = Minions('res_file.tgz', tracker=TRACKER)
        minion_uploader.upload_res('./res_file.tgz')
        time.sleep(1)
        port = minion_uploader.port
        import logging
        logging.basicConfig(level=logging.ERROR)

        def foo(session):
            size = 1024 ** 2
            r = session.get(
                "http://localhost:%s/?res_url=res_file.tgz" % port,
                stream=True,
                headers={"Range": "bytes=0-%s" % (size)},
                timeout=2)
            # r.raw.close()
            r.content
            return r

        try:
            s = requests.Session()
            g = []
            for i in range(500):
                g.append(gevent.spawn(foo, s))

            gevent.sleep(1)
            for i in g:
                i.get()
                # print "------------"
                # r = s.get("http://localhost:%s/?res_url=res_file.tgz" % port,
                #     stream=True,
                #     headers={"Range": "bytes=0-%s" %(size)},
                #     timeout=1)

                # print len(r.raw.read(1023))
                # len(r.raw.read(1024**2 + 1))
                # time.sleep(1)
                # r.content
                # print r
                # r.raw.close()
                # r.close()
                # r.raw.close()
                # del r
                # r = s.get(
                #     "http://localhost:%s/?res_url=res_file.tgz&pieces=all" %
                #         port,
                #     stream=True,
                #     headers={"Range": "bytes=0-%s" % (size)},
                #     timeout=1)
                # #print len(r.raw.read(1024**2 * 2 + 1))
                # r.close()

                # r.content
                # r = s.get(
                # "http://localhost:%s/?res_url=res_file.tgz&pieces=all"
                # % port,
                #     stream=True,
                #     headers={"Range": "bytes=0-%s" % (size)},
                #     timeout=1)
                # r.content
                # r.close()
                # print "------------"
        finally:
            print 'finally'
            minion_uploader.close()
            minion_uploader.stop_upload()

    def test_keepalive(self):
        minion_uploader = Minions('res_file.tgz', tracker=TRACKER)
        minion_uploader.upload_res('./res_file.tgz')
        time.sleep(1)

        port = minion_uploader.port

        try:
            # test if whether get block api interface works
            r = requests.get(
                "http://localhost:%s/?res_url=res_file.tgz&pieces=all" % port,
                stream=True,
                headers={"Range": "bytes=0-1"},
                timeout=1)
            print r.content

            s = socket.socket()
            s.settimeout(0.2)
            s.connect(('localhost', port))
            s.send(
                "GET /?res_url=res_file.tgz HTTP/1.1\r\n"
                "Host: localhost:62630\r\n"
                "Accept-Encoding: identity\r\n"
                "Content-Length: 0\r\nRange: bytes=0-1,3-4\r\n"
                "\r\n")
            print s.recv(200)
            s.recv(1024 ** 2 + 400)

            with self.assertRaises(socket.timeout):
                s.recv(1024)

            s.send(
                "GET /?res_url=res_file.tgz HTTP/1.1\r\n"
                "Host: localhost:62630\r\n"
                "Accept-Encoding: identity\r\n"
                "Content-Length: 0\r\n"
                "Range: bytes=0-1,3-4\r\n"
                "\r\n")
            print s.recv(200)

            # conn = httplib.HTTPConnection("localhost", port)
            # conn.debuglevel = 1
            # conn.request(
            # "GET", "/?res_url=res_file.tgz",
            # "", headers={"Range": "bytes=0-1,3-4"})
            # r = conn.getresponse()

        finally:
            minion_uploader.close()
            minion_uploader.stop_upload()

    def test_multipart(self):
        minion_uploader = Minions('res_file.tgz', tracker=TRACKER)
        minion_uploader.upload_res('./res_file.tgz')

        time.sleep(1)
        port = minion_uploader.port

        try:
            r = requests.get(
                    "http://localhost:%s/?res_url=res_file.tgz" % port,
                    headers={"Range": "bytes=0-1,3-4"})

            self.assertEqual(len(r.content), 4)
        finally:
            minion_uploader.close()
            minion_uploader.stop_upload()

    def tearDown(self):
        self.server_process.terminate()
