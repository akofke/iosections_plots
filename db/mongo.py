import csv
import os

from pymongo import MongoClient

from db import RESOURCE_NAMES, GPFS_NAME

MONGO_URI = os.environ.get("MONGO_URI")

def extract_jobs_data(resource):
    client = MongoClient(MONGO_URI)
    db = client.supremm
    res = db[resource]

    return res.find({"$or": [{"gpfs.GSS-cbls-ccr-buffalo-edu:gpfs0.read_bytes.avg": {"$gt": 0}},
                             {"gpfs.GSS-cbls-ccr-buffalo-edu:gpfs0.write_bytes.avg": {"$gt": 0}}],
                     "iosections.error": {"$exists": False}},
                    projection=["iosections", "acct", "gpfs"])


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
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)
        for job in jobs:
            acct = job['acct']
            row = [acct[f] for f in ("resource_id", "local_job_id", "end_time")]

            iosections = job["iosections"]
            stats_read = iosections["section_stats_read"]
            stats_write = iosections["section_stats_write"]

            row.extend(sect['avg'] for sect in stats_read)
            row.extend(sect['avg'] for sect in stats_write)
            row.extend(sect['avg'] for sect in iosections['section_start_timestamps'])
            row.extend((job['gpfs'][GPFS_NAME]['read_bytes']['avg'], job['gpfs'][GPFS_NAME]['write_bytes']['avg']))

            writer.writerow(row)

def main():
    for res in RESOURCE_NAMES:
        print(f"starting {res}")
        write_csv(extract_jobs_data(res), res)

if __name__ == '__main__':
    main()
