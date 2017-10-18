#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
from multiprocessing import Pool


NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class Worker(object):

    def __init__(self, device_memc, dry):
        self.device_memc = device_memc
        self.dry = dry
        self.memclients = {}

    def __call__(self, fname):
        processed = errors = 0
        logging.info('Processing %s' % fname)
        with gzip.open(fname) as fd:
            for line in fd:
                line = line.strip()
                if not line:
                    continue
                appsinstalled = self.parse_appsinstalled(fname, line)
                if not appsinstalled:
                    errors += 1
                    continue
                memc_addr = self.device_memc.get(appsinstalled.dev_type)
                if not memc_addr:
                    errors += 1
                    logging.error("Processing %s. Unknow device type: %s" % (fname, appsinstalled.dev_type))
                    continue
                ok = self.insert_appsinstalled(fname, memc_addr, appsinstalled)
                if ok:
                    processed += 1
                else:
                    errors += 1

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Processing %s. Acceptable error rate (%s). Successfull load" % (fname, err_rate))
        else:
            logging.error("Processing %s. High error rate (%s > %s). Failed load" % (fname, err_rate, NORMAL_ERR_RATE))
        return fname

    def insert_appsinstalled(self, fname, memc_addr, appsinstalled):
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        if self.dry:
            logging.debug("Processing %s. %s - %s -> %s" % (fname, memc_addr, key, str(ua).replace("\n", " ")))
            return True
        else:
            memc = self.memclients.get(memc_addr)
            if memc is None:
                memc = memcache.Client([memc_addr])
                self.memclients[memc_addr] = memc
            ok = memc.set(key, packed)
            for _ in xrange(3):
                if ok:
                    break
                ok = memc.set(key, packed)
            return ok

    def parse_appsinstalled(self, fname, line):
        line_parts = line.strip().split("\t")
        if len(line_parts) < 5:
            return
        dev_type, dev_id, lat, lon, raw_apps = line_parts
        if not dev_type or not dev_id:
            return
        try:
            apps = [int(a.strip()) for a in raw_apps.split(",")]
        except ValueError:
            apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
            logging.info("Processing %s. Not all user apps are digits: `%s`" % (fname, line))
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            logging.info("Processing %s. Invalid geo coords: `%s`" % (fname, line))
        return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    fnames = glob.glob(options.pattern)
    fnames = sorted(fnames)
    for fname in Pool(options.workers).imap(Worker(device_memc, options.dry), fnames):
        head, fn = os.path.split(fname)
        os.rename(fname, os.path.join(head, "." + fn))


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--workers", action="store", type="int", default=2)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception, e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)

