# -*- coding: utf-8 -*-

import json
import socket
import urllib2


SECRET_FILE = '/usr/local/etc/ip2w/secret.json'


def get_coord_by_ip(ip):
    url = 'https://ipinfo.io/%s/loc' % ip
    resp = urllib2.urlopen(url, timeout=20).read()
    try:
        numbers = [float(n.strip()) for n in resp.split(',')]
        assert len(numbers) == 2
    except:
        raise ValueError('IPINFO.IO needs your money: (%s).' % resp)
    return numbers


def get_weather(lat, lon, apikey):
    url = 'http://api.openweathermap.org/data/2.5/weather?lat=%s&lon=%s&APPID=%s' % (lat, lon, apikey)
    resp = urllib2.urlopen(url, timeout=20).read()
    resp = json.loads(resp)
    city = resp['name']
    temp = float(resp['main']['temp']) - 273.15
    conditions = resp['weather'][0]['description']
    return {'city': city, 'temp': temp, 'conditions': conditions}


def application(environ, start_response):
    uri = environ['REQUEST_URI']
    ip = uri.split('/')[-1]
    try:
        try:
            socket.inet_aton(ip)
        except socket.error:
            raise ValueError('IP adderss is not valid.')
        lat, lon = get_coord_by_ip(ip)
        with open(SECRET_FILE) as fd:    
            apikey = json.loads(fd.read())['apikey']
        weather = get_weather(lat, lon, apikey)
        weather = json.dumps(weather)
    except Exception, err:
        err = json.dumps({'error': str(err)})
        length = len(err)
        start_response('400 Bad Request', [('Content-Type', 'application/json'),
                                           ('Content-Length', str(length))])
        return [err]
    length = len(weather)
    start_response('200 OK', [('Content-Type', 'application/json'),
                              ('Content-Length', str(length))])
    return [weather]

