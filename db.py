import csv
import os

import MySQLdb
import MySQLdb.cursors
from pymongo import MongoClient

GPFS_NAME = 'GSS-cbls-ccr-buffalo-edu:gpfs0'
MONGO_URI = os.environ.get("MONGO_URI")
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")

RESOURCE_NAMES = {"resource_11", "resource_13", "resource_9", "resource_8", "resource_10"}

db = MySQLdb.connect(host=MYSQL_HOST, port=3306, user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PW'],
                     cursorclass=MySQLdb.cursors.DictCursor)

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
