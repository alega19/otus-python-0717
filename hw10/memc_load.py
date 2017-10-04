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
from threading import Thread, Lock
from heapq import heappop, heappush


NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class FileState:

    def __init__(self, fn):
        self.fn = fn
        self.processed = False

    def __lt__(self, other):
        return self.fn < other.fn


class Worker(Thread):

    lock = Lock()
    fnames_iter = None
    device_memc = None
    dry = None

    file_states_lock = Lock()
    file_states = []

    def __init__(self):
        super(Worker, self).__init__()
        self.memclients = {}
        self.cur_fn = None
        self.daemon = True

    def next_file(self):
        try:
            with self.lock:
                self.cur_fn = next(self.fnames_iter)
        except StopIteration:
            self.cur_fn = None

    def run(self):
        while True:
            self.next_file()
            if self.cur_fn is None:
                break
            state = FileState(self.cur_fn)
            with self.file_states_lock:
                heappush(self.file_states, state)

            processed = errors = 0
            logging.info('Processing %s' % self.cur_fn)
            with gzip.open(self.cur_fn) as fd:
                for line in fd:
                    line = line.strip()
                    if not line:
                        continue
                    appsinstalled = self.parse_appsinstalled(line)
                    if not appsinstalled:
                        errors += 1
                        continue
                    memc_addr = self.device_memc.get(appsinstalled.dev_type)
                    if not memc_addr:
                        errors += 1
                        logging.error("Processing %s. Unknow device type: %s" % (self.cur_fn, appsinstalled.dev_type))
                        continue
                    ok = self.insert_appsinstalled(memc_addr, appsinstalled)
                    if ok:
                        processed += 1
                    else:
                        errors += 1
            with self.file_states_lock:
                state.processed = True
            self.dot_rename()

            err_rate = float(errors) / processed
            if err_rate < NORMAL_ERR_RATE:
                logging.info("Processing %s. Acceptable error rate (%s). Successfull load" % (self.cur_fn, err_rate))
            else:
                logging.error("Processing %s. High error rate (%s > %s). Failed load" % (fn, err_rate, NORMAL_ERR_RATE))

    def insert_appsinstalled(self, memc_addr, appsinstalled):
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        try:
            if self.dry:
                logging.debug("Processing %s. %s - %s -> %s" % (self.cur_fn, memc_addr, key, str(ua).replace("\n", " ")))
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
                if not ok:
                    raise RuntimeError()
        except Exception, e:
            logging.exception("Processing %s. Cannot write to memc %s: %s" % (self.cur_fn, memc_addr, e))
            return False
        return True

    def parse_appsinstalled(self, line):
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
            logging.info("Processing %s. Not all user apps are digits: `%s`" % (self.cur_fn, line))
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            logging.info("Processing %s. Invalid geo coords: `%s`" % (self.cur_fn, line))
        return AppsInstalled(dev_type, dev_id, lat, lon, apps)

    def dot_rename(self):
        with self.file_states_lock:
            while self.file_states:
                state = heappop(self.file_states)
                if state.processed:
                    head, fn = os.path.split(state.fn)
                    os.rename(state.fn, os.path.join(head, "." + fn))
                else:
                    heappush(self.file_states, state)
                    break


def main(options):
    Worker.device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    Worker.fnames_iter = glob.iglob(options.pattern)
    Worker.dry = options.dry
    workers = []
    for _ in xrange(options.workers):
        worker = Worker()
        worker.start()
        workers.append(worker)
    for worker in workers:
        worker.join()


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
