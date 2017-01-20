import json
from flask import Flask
from flask import request
from models import PeerList

peers = PeerList()
app = Flask(__name__)

from flask import Flask, request, session, g, redirect, url_for, \
             abort, render_template, flash

# configuration
"""
{'resource_name':[(ip, port), ]}
"""

@app.route("/peer/", methods=["GET", "POST", "DELETE"])
def peer():
    adt_info = None
    response = dict()
    if request.method == "POST":
        data = request.json
        try:
            adt_info = data['adt_info']
        except KeyError:
            pass

        ip = data['ip']
        port = data['port']
        port = int(port)
        res = data['res']
        peers.add(res, (ip, port))
        response['ret'] = 'success'

    elif request.method == "GET":
        res = request.args.get('res')
        if res:
            ava_peers = peers.get_peers(res, num=40)
            response[res] = ava_peers
        else:
            response = json.dumps(peers)

    elif request.method == "DELETE":
        res = request.args.get('res')
        ip = request.args.get('ip')
        port = request.args.get('port')
        port = int(port)
        peers.delete(res, (ip, port))
        response['ret'] = 'success'

    return json.dumps(response, indent=4)

@app.route("/res/")
def res():
    response = dict()
    if request.method == "GET":
        res = peers.keys()
        response['resources'] = res
    return json.dumps(response, indent=4)

@app.route("/debug/")
def debug():
    import pdb
    pdb.set_trace()
    return 'debuging'

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=6000, threaded=True)
