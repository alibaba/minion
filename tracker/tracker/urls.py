from django.conf.urls import patterns, include, url
from django.views.decorators.csrf import csrf_exempt
from django.contrib import admin
from peer.views import PeerView, ResView

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'tracker.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    (r'^res/', csrf_exempt(ResView.as_view())),
    (r'^peer/', csrf_exempt(PeerView.as_view())),
)
