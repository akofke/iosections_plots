import csv
import os

from pymongo import MongoClient

from db import RESOURCE_NAMES, GPFS_NAME

MONGO_URI = os.environ.get("MONGO_URI")

# QUERY = {
#     "$or": [{"gpfs.GSS-cbls-ccr-buffalo-edu:gpfs0.read_bytes.avg": {"$gt": 0}},
#             {"gpfs.GSS-cbls-ccr-buffalo-edu:gpfs0.write_bytes.avg": {"$gt": 0}}],
#     "iosections.error": {"$exists": False}
# }

QUERY = {
    "timeseries_patterns_gpfs": {"$exists": True},
    "timeseries_patterns_gpfs.gpfs-fsios-read_bytes.error": {"$exists": False},
    "timeseries_patterns_gpfs.gpfs-fsios-write_bytes.error": {"$exists": False},
}

PROJECTION = ["timeseries_patterns_gpfs", "acct", "gpfs"]

client = MongoClient(MONGO_URI)
db = client.supremm


def extract_jobs_data(resource):
    res = db[resource]

    return res.find(QUERY, projection=PROJECTION)


def extract_autoperiod_jobs(resource):
    res = db[resource]

    q = dict(QUERY, **{"iosections.node_periods": {"$exists": True}, "iosections._version": 1})

    return res.find(q, projection=PROJECTION)


def write_csv(jobs, res_name):
    with open(f"{res_name}.csv", 'w', newline='') as csvfile:
        fieldnames = [
            "res_id",
            "local_job_id",
            "end_timestamp",
        ]

        fieldnames.extend(f'r{n}' for n in range(4))
        fieldnames.extend(f'w{n}' for n in range(4))
        fieldnames.extend(f'ts{n}' for n in range(4))
        fieldnames.extend(('gpfs_read', 'gpfs_write'))
        fieldnames.extend(('read_variability', 'period_r', 'period_score_r', 'on_period_sum_r', 'off_period_sum_r'))
        fieldnames.extend(('write_variability', 'period_w', 'period_score_w', 'on_period_sum_w', 'off_period_sum_w'))

        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)
        for job in jobs:
            acct = job['acct']
            row = [acct[f] for f in ("resource_id", "local_job_id", "end_time")]

            patterns = job["timeseries_patterns_gpfs"]
            stats_read = patterns["gpfs-fsios-read_bytes"]["sections"]
            stats_write = patterns["gpfs-fsios-read_bytes"]["sections"]

            row.extend(sect['avg'] for sect in stats_read)
            row.extend(sect['avg'] for sect in stats_write)
            row.extend(sect['avg'] for sect in patterns['gpfs-fsios-read_bytes']['section_start_timestamps'])
            row.extend((job['gpfs'][GPFS_NAME]['read_bytes']['avg'], job['gpfs'][GPFS_NAME]['write_bytes']['avg']))

            key = lambda stat: stat['avg']
            for mname in ('gpfs-fsios-read_bytes', 'gpfs-fsios-write_bytes'):
                metric = patterns[mname]

                maxavg = max(metric['sections'], key=key)['avg']
                minavg = min(metric['sections'], key=key)['avg']
                var = (maxavg - minavg) / maxavg if maxavg != 0 else 0

                ap = metric['autoperiod']
                period = ap['period'] if ap else "null"
                score = ap['normalized_score'] if ap else "null"
                on_period_sum = ap['on_period']['sum'] if ap else "null"
                off_period_sum = ap['off_period']['sum'] if ap else "null"

                row.extend((var, period, score, on_period_sum, off_period_sum))

            writer.writerow(row)


def main():
    for res in RESOURCE_NAMES:
        print(f"starting {res}")
        write_csv(extract_jobs_data(res), res)


if __name__ == '__main__':
    main()
