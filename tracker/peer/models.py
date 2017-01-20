import random
from hashlib import md5

from django.db import models
from collections import OrderedDict

class Peer(models.Model):
    ip = models.IPAddressField(db_index=True)
    port = models.IntegerField(db_index=True)
    res = models.TextField(max_length=200)
    res_md5 = models.CharField(max_length=32, db_index=True)
    site = models.CharField(max_length=20, db_index=True)

    @classmethod
    def new(cls, ip, port, res=None, res_md5=None, site=None):
        if res:
            m = md5()
            m.update(res)
            res_md5 = m.hexdigest()

        return cls(ip=ip,
            port=port,
            res_md5=res_md5,
            site=site
        )


    def get_peers(cls, res_md5, site=None, num=None):
        peers = cls.object.filter(res_md5=res_md5)
        if site:
            peers.filter(site=site)

        if num:
            peers_num = len(peers)
            sample_num = min(peers_num, num)
            sample = random.sample(xrange(peers_num), sample_num)
        else:
            return peers

    def to_dict(self):
        if self.site:
            return [self.ip, self.port, {"site": self.site}]
        else:
            return [self.ip, self.port]

