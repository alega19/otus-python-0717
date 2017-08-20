# -*- coding: utf-8 -*-

import json
import socket
import urllib2
from functools import wraps
from time import sleep


SECRET_FILE = '/usr/local/etc/ip2w/secret.json'


class InvalidIPError(ValueError):
    pass

def retry(count, delay_in_seconds):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kvargs):
            for _ in xrange(count):
                try:
                    return fn(*args, **kvargs)
                except:
                    sleep(delay_in_seconds)
            return fn(*args, **kvargs)
        return wrapper
    return deco


@retry(3, 5)
def get_coord_by_ip(ip):
    url = 'https://ipinfo.io/%s/loc' % ip
    resp = urllib2.urlopen(url, timeout=20).read()
    try:
        numbers = [float(n.strip()) for n in resp.split(',')]
        assert len(numbers) == 2
    except:
        raise ValueError('IPINFO.IO needs your money: (%s).' % resp)
    return numbers


@retry(3, 5)
def get_weather(lat, lon, apikey):
    url = 'http://api.openweathermap.org/data/2.5/weather?lat=%s&lon=%s&APPID=%s' % (lat, lon, apikey)
    resp = urllib2.urlopen(url, timeout=20).read()
    resp = json.loads(resp)
    city = resp['name']
    temp = float(resp['main']['temp']) - 273.15
    conditions = resp['weather'][0]['description']
    return {'city': city, 'temp': temp, 'conditions': conditions}


def parse_ip(ip):
    try:
        socket.inet_aton(ip)
    except socket.error:
        raise InvalidIPError('IP adderss "%s" is not valid.' % ip)


def application(environ, start_response):
    uri = environ['REQUEST_URI']
    ip = uri.split('/')[-1]
    try:
        parse_ip(ip)
        lat, lon = get_coord_by_ip(ip)
        with open(SECRET_FILE) as fd:    
            apikey = json.loads(fd.read())['apikey']
        weather = get_weather(lat, lon, apikey)
        status = '200 OK'
        body = json.dumps(weather)
    except InvalidIPError, err:
        status = '400 Bad Request'
        body = json.dumps({'error': str(err)})
    except Exception, err:
        status = '500 Internal Server Error'
        body = json.dumps({'error': str(err)})

    start_response(status, [('Content-Type', 'application/json'),
                            ('Content-Length', str(body))])
    return [body]

