import time
import subprocess
from multiprocessing import Process

import django
from django.core.management import call_command

#django.conf.settings.configure("tracker.tracker.settings")

def runserver(ip, port):
    subprocess.call(['python', 'tracker/manage.py', 'flush', '--noinput'])

    p = subprocess.Popen(['python', 'tracker/manage.py', 'runserver',
                '%s:%s' % (ip, port), '--noreload'])
    time.sleep(0.4)
    return p

#if __name__ == '__main__':
#    runserver('localhost', 5001)
