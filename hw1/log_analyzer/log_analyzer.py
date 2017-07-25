#!/usr/bin/env python
# -*- coding: utf-8 -*-

# {"count": 2767, "time_avg": 62.994999999999997, "time_max": 9843.5689999999995, "time_sum": 174306.35200000001,
#  "url": "/api/v2/internal/html5/phantomjs/queue/?wait=1m", "time_med": 60.073, "time_perc": 9.0429999999999993,
#  "count_perc": 0.106}

# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

import re
import os
import gzip
import shutil
import json


config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log"
}


class LogFile:

    def __init__(self, fpath):
        self._fpath = fpath

    def report_name(self):
        fname = os.path.basename(self._fpath)
        date_str = fname[20:28]
        res = 'report-{0}.{1}.{2}.html'.format(date_str[:4], date_str[4:6], date_str[6:])
        return res

    def read(self):
        if self._fpath[-2:] == 'gz':
            temp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'TEMP')
            tmp_f = open(temp_path, 'wb')
            try:
                with gzip.open(self._fpath, 'rb') as log_f:
                    shutil.copyfileobj(log_f, tmp_f)
                tmp_f.close()
                for line in open(temp_path):
                    yield line
            finally:
                if not tmp_f.closed:
                    tmp_f.close()
                os.remove(temp_path)
        else:
            for line in open(self._fpath):
                yield line

    @staticmethod
    def last_logfile(log_dir):
        fnames = os.listdir(log_dir)
        regex = re.compile(r'nginx-access-ui\.log-\d{8}')
        log_names = [fn for fn in fnames if regex.match(fn[:28])]
        if log_names:
            log_name = sorted(log_names)[-1]
            log_path = os.path.join(log_dir, log_name)
            return LogFile(log_path)
        else:
            return None


def parse(lines):
    regex = re.compile(r'"\S+ (\S+).*" \d+ \d+ ".+" ".+" ".+" ".+" ".+" ([.\d]+)$')
    for line in lines:
        match = regex.search(line)
        if match:
            url, req_time = match.groups()
            yield {'url': url, 'req_time': float(req_time)}


def get_stats(records):
    stats = {}
    for record in records:
        url = record['url']
        req_time = record['req_time']
        req_time_history = stats.get(url)
        if req_time_history is None:
            stats[url] = [req_time]
        else:
            stats[url].append(req_time)
    return stats


def get_rows(stats, max_size):

    def med(values):
        values = sorted(values)
        if len(values) % 2 == 1:
            index = len(values) // 2
            return values[index]
        else:
            index0 = len(values) // 2
            index1 = index0 - 1
            return (values[index0] + values[index1]) / 2.0

    rows = []
    time_total = 0
    requests_total = 0
    for url in stats:
        req_time_history = stats.get(url)
        requests_count = len(req_time_history)
        time_sum = sum(req_time_history)
        row = {
            "url": url,
            "count": requests_count,
            "time_avg": time_sum / requests_count,
            "time_max": max(req_time_history),
            "time_sum": time_sum,
            "time_med": med(req_time_history),
            "time_perc": None,
            "count_perc": None}
        rows.append(row)
        time_total += time_sum
        requests_total += requests_count
    for row in rows:
        row["time_perc"] = 100.0 * row["time_sum"] / time_total
        row["count_perc"] = 100.0 * row["count"] / requests_total
    rows = sorted(rows, key=lambda r: r['time_sum'], reverse=True)
    return rows[:max_size]


def save_report(rows, file_path):
    with open(file_path, 'w') as f:
        table_as_string = json.dumps(rows)
        template = open('./report.html').read()
        s = template.replace('$table_json', table_as_string)
        f.write(s)


def main(conf):
    if not os.path.exists(conf['REPORT_DIR']):
        os.makedirs(conf['REPORT_DIR'])
    logfile = LogFile.last_logfile(conf['LOG_DIR'])
    if logfile is None:
        return
    report_path = os.path.join(conf['REPORT_DIR'], logfile.report_name())
    if not os.path.exists(report_path):
        lines = logfile.read()
        records = parse(lines)
        stats_by_url = get_stats(records)
        rows = get_rows(stats_by_url, conf['REPORT_SIZE'])
        save_report(rows, report_path)


if __name__ == "__main__":
    main(config)
