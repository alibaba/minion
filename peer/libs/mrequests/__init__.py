
from gevent import monkey

monkey.patch_all(thread=False, select=False)

import requests
