import json
import random
from hashlib import md5
from .models import Peer

import cProfile, StringIO, pstats

from django.db import transaction
from django.views.generic import View
from django.shortcuts import render, HttpResponse

def random_sample(iter_obj, t_num, num):
    sample_num = min(t_num, num)
    _sample = random.sample(xrange(t_num), sample_num)
    return [iter_obj[i] for i in _sample]

def get_random_sample_ids(t_num, num):
    t_num = int(t_num)
    num = int(num)
    sample_num = min(t_num, num)
    _sample = random.sample(xrange(t_num), sample_num)
    return _sample

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


class MD5Cache(object):
    def __init__(self):
        self._dict = {}

    def get_md5(self, res):
        try:
            return self._dict[res]
        except KeyError:
            m = md5()
            m.update(res)
            res_md5 = m.hexdigest()
            self._dict[res] = res_md5
            return res_md5

md5_cache = MD5Cache()


class PeerView(View):
    #@mprofile
    def get(self, request):
        #import cProfile, pstats, StringIO
        #pr = cProfile.Profile()
        #pr.enable()

        data = request.GET

        res = data['res']
        num = data.get('num', 40)
        verbose = data.get('verbose', 0)

        ret = {}
        res_md5 = md5_cache.get_md5(res)
        peers = Peer.objects.filter(res_md5=res_md5)
        try:
            strict = request.GET['strict']
        except KeyError:
            strict = False

        #with transaction.atomic():
        if strict:
            adt_info = json.loads(request.body)['adt_info']
            if strict == 'site':
                site = adt_info['site']
                peers = peers.filter(site=site)

        #    # solution 1
        #    #peers = random_sample(peers, peers.count(), num)

        #    # solution 2
        #    #peers = peers.order_by('?')[:num]

        # solution 3
        pk_list = peers.values_list('pk', flat=True)
        pk_list.count()
        pk_list = list(pk_list)
        ids = get_random_sample_ids(len(pk_list), num)
        #from django.db import connection
        #print connection.queries
        #print time.time() - begin
        sample_pk_list = [pk_list[id] for id in ids]
        peers = peers.filter(id__in=sample_pk_list)

            #from django.db import connection
            #print connection.queries

        ret[res] = []
        for p in peers:
            if verbose:
                ret[res].append(p.to_dict())
            else:
                ret[res].append((p.ip, p.port))

        ret['ret'] = 'success'
        #import StringIO
        #pr.disable()
        #s = StringIO.StringIO()
        #sortby = 'cumulative'
        #ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        #ps.print_stats()
        #print s.getvalue()
        return HttpResponse(json.dumps(ret))

    def post(self, request):
        data = json.loads(request.body)

        ip = data['ip']
        port = int(data['port'])
        res = data['res']

        m = md5()
        m.update(res)
        res_md5 = m.hexdigest()

        hostname = None
        site = None
        try:
            adt_info = data['adt_info']
            site = adt_info.get('site', None)
            hostname = adt_info.get('hostname', None)
        except KeyError:
            pass


        p, created = Peer.objects.get_or_create(ip=ip,
            port=port,
            res=res,
            res_md5=res_md5)

        if site:
            p.site = site

        p.save()
        return HttpResponse(json.dumps({'ret':'success'}))

    def delete(self, request):
        data = request.GET

        ip = data['ip']
        port = int(data['port'])
        res = data['res']

        Peer.objects.filter(ip=ip, port=port, res=res).delete()

        return HttpResponse(json.dumps({'ret':'success'}))

class ResView(View):
    def get(self, request):
        data = request.GET
        response = {}
        peers = Peer.objects.values('res').distinct()
        response['resources'] = []
        response['ret'] = 'success'
        for p in peers:
            response['resources'].append(p['res'])

        return HttpResponse(json.dumps(response))
