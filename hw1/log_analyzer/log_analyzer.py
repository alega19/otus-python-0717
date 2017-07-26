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

    def filename(self):
        return os.path.basename(self._fpath)

    def read_lines(self):
        if self._fpath[-2:] == 'gz':
            log_f = gzip.open(self._fpath)
        else:
            log_f = open(self._fpath)
        with log_f:
            for line in log_f:
                yield line

    @classmethod
    def last_logfile(cls, log_dir):
        fnames = os.listdir(log_dir)
        regex = re.compile(r'nginx-access-ui\.log-\d{8}')
        log_names = [fn for fn in fnames if regex.match(fn[:28])]
        if log_names:
            log_name = sorted(log_names)[-1]
            log_path = os.path.join(log_dir, log_name)
            return cls(log_path)
        else:
            return None


def get_report_name(log_fname):
    date_str = log_fname[20:28]
    res = 'report-{0}.{1}.{2}.html'.format(date_str[:4], date_str[4:6], date_str[6:])
    return res


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


def median(values):
    values = sorted(values)
    if len(values) % 2 == 1:
        index = len(values) // 2
        return values[index]
    else:
        index0 = len(values) // 2
        index1 = index0 - 1
        return (values[index0] + values[index1]) / 2.0


def get_rows(stats, max_size):
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
            "time_med": median(req_time_history),
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
    with open('./report.html') as template_f, open(file_path, 'w') as report_f:
        table_as_string = json.dumps(rows)
        template = template_f.read()
        s = template.replace('$table_json', table_as_string)
        report_f.write(s)


def main(conf):
    if not os.path.exists(conf['REPORT_DIR']):
        os.makedirs(conf['REPORT_DIR'])
    logfile = LogFile.last_logfile(conf['LOG_DIR'])
    if logfile is None:
        return
    report_name = get_report_name(logfile.filename())
    report_path = os.path.join(conf['REPORT_DIR'], report_name)
    if not os.path.exists(report_path):
        lines = logfile.read_lines()
        records = parse(lines)
        stats_by_url = get_stats(records)
        rows = get_rows(stats_by_url, conf['REPORT_SIZE'])
        save_report(rows, report_path)


if __name__ == "__main__":
    main(config)
