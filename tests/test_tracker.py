import time
import json
import unittest
import socket
import struct

import requests

host = "localhost"
port = 5001
peer_url = "http://%s:%s/peer/" % (host, port)


def int_to_ipstr(int_ip):
    return socket.inet_ntoa(
            struct.pack('I', socket.htonl(int_ip))
            )


def ipstr_to_int(str_ip):
    return socket.ntohl(
            struct.unpack("I", socket.inet_aton(str(str_ip)))[0]
            )


class TrackerServerTest(unittest.TestCase):
    def setUp(self):
        from tracker import runserver
        self.server_process = runserver(host, port)
        time.sleep(1.)

        data = {'ip': "1.1.1.1", 'port': 80, "res": '1.tgz'}
        requests.post(
            peer_url, data=json.dumps(data),
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )

    def test_add_peer(self):
        data = {'ip': "1.1.1.1", 'port': 80, "res": '1.tgz'}
        r = requests.post(
            peer_url, data=json.dumps(data),
            headers={'Content-Type': 'application/json; charset=utf-8'})
        r.raise_for_status()

    def test_get_peer_strict(self):
        data = {
            'ip': "1.1.1.1", 'port': 80, "res": '1.tgz',
            'adt_info': {'site': 'mysite10'}
        }

        r = requests.post(
                peer_url, data=json.dumps(data),
                headers={'Content-Type': 'application/json; charset=utf-8'})

        data = {
                'ip': "1.1.1.2", 'port': 80, "res": '1.tgz',
                'adt_info': {'site': 'c.mysite10'}
        }

        r = requests.post(
                peer_url, data=json.dumps(data),
                headers={'Content-Type': 'application/json; charset=utf-8'})

        print requests.get(peer_url+"?res=1.tgz&verbose=1").content
        r = requests.get(
                peer_url+"?res=1.tgz&strict=site",
                data=json.dumps({"adt_info": {"site": "mysite10"}}))

        content = {
            "1.tgz": [
                [
                    "1.1.1.1",
                    80
                ]],
            "ret":
                'success'
            }
        self.assertEqual(r.json(), content)

    def test_unknow(self):
        data = {
            'ip': "1.1.1.1", 'port': 80, "res": '1.tgz',
            'adt_info': {'site': 'unknow'}
        }

        requests.post(
            peer_url, data=json.dumps(data),
            headers={'Content-Type': 'application/json; charset=utf-8'})

        data = {
            'ip': "1.1.1.2", 'port': 80, "res": '1.tgz',
            'adt_info': {'site': 'unknow'}
        }

        requests.post(
            peer_url, data=json.dumps(data),
            headers={'Content-Type': 'application/json; charset=utf-8'})

        print requests.get(peer_url+"?res=1.tgz&verbose=1").content

        requests.get(
            peer_url+"?res=1.tgz&strict=site",
            data=json.dumps({"adt_info": {"site": "unknow"}}))

        # content = {
        #     "1.tgz": [
        #         [
        #             "1.1.1.1",
        #             80
        #         ]],
        #     "ret" :
        #         'success'
        #     }

        # self.assertEqual(r.json(), content)

    def test_get_peer(self):
        data = {'ip': "1.1.1.1", 'port': 80, "res": '1.tgz'}
        requests.post(
            peer_url, data=json.dumps(data),
            headers={'Content-Type': 'application/json; charset=utf-8'})
        r = requests.get(peer_url+"?res=1.tgz")
        content = {
            "1.tgz": [
                [
                    "1.1.1.1",
                    80
                ]],
            "ret":
                'success'
            }
        self.assertEqual(r.json(), content)

    def test_get_peer_performance(self):
        data = {'ip': "1.1.1.1", 'port': 80, "res": '1.tgz'}
        for i in range(4000):
            data['port'] = i
            r = requests.post(
                    peer_url, data=json.dumps(data),
                    headers={
                        'Content-Type': 'application/json; charset=utf-8'})

        # ip_start = ipstr_to_int('1.1.1.1')
        # for i in range(5):
        #     data = {'ip': int_to_ipstr(ip_start + i),
        # 'port': 80, "res": '1.tgz'}
        #     r = requests.post(peer_url, data=json.dumps(data),
        #         headers={'Content-Type': 'application/json; charset=utf-8'})

        start = time.time()
        r = requests.get(peer_url+"?res=1.tgz&num=140")
        print r.content
        elasp = time.time() - start
        print 'use %s(s)' % elasp

    def test_get_peer_less_tw(self):
        for i in range(42):
            data = {'ip': "1.1.1.1" + str(i), 'port': 80, "res": '1.tgz'}
            r = requests.post(
                peer_url, data=json.dumps(data),
                headers={'Content-Type': 'application/json; charset=utf-8'})

        r = requests.get(peer_url+"?res=1.tgz")
        self.assertEqual(len(r.json()['1.tgz']), 40)

    def test_del_peer(self):
        r = requests.delete(peer_url+"?res=1.tgz&ip=1.1.1.1&port=80")
        r.raise_for_status()

    def tearDown(self):
        self.server_process.terminate()


if __name__ == '__main__':
    unittest.main()
