import random
from copy import copy

class PeerList(dict):

    def add(self, res, ip_port):
        try:
            if ip_port in self[res]:
                return
            self[res].append(ip_port)
        except KeyError:
            self[res] = [ip_port]

    def delete(self, res, ip_port):
        try:
            self[res].remove(ip_port)
        except ValueError:
            pass

    def get_peers(self, res, num=None):
        try:
            peers = copy(self[res])
            if num:
                peers_num = len(peers)
                sample_num = peers_num if peers_num < num else num
                sample = random.sample(xrange(peers_num), sample_num)
                return [peers[i] for i in sample]
            else:
                return peers
        except KeyError:
            return []

