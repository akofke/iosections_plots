from pymongo import MongoClient
import matplotlib.pyplot as plt
import numpy as np
import os

GPFS_NAME = "GSS-cbls-ccr-buffalo-edu:gpfs0"
MONGO_URI = os.environ["MONGO_URI"]


def normalize_inf(ratios):
    rmax = max(filter(lambda x: x != float("inf"), ratios))
    return [i if i != float("inf") else rmax for i in ratios]


def mean(lst):
    return sum(i["avg"] for i in lst) / len(lst)

client = MongoClient(MONGO_URI)
db = client.supremm
res = db.resource_11

jobs = res.find({"$or": [{"gpfs.GSS-cbls-ccr-buffalo-edu:gpfs0.read_bytes.avg": {"$gt": 0},
                          "gpfs.GSS-cbls-ccr-buffalo-edu:gpfs0.write_bytes.avg": {"$gt": 0}}],
                 "iosections.error": {"$exists": False}},
                projection=["iosections", "gpfs.GSS-cbls-ccr-buffalo-edu:gpfs0"])

ratios = []

data_totals_read = []
data_totals_write = []

start_middle_diffs_read = []
start_middle_diffs_write = []

caps_middle_diffs_read = []
caps_middle_diffs_write = []

middle_end_diffs_read = []
middle_end_diffs_write = []

for job in jobs:
    if job['gpfs'][GPFS_NAME]['read_bytes']['avg'] < 100*1024*1024:
        continue
    iosections = job["iosections"]
    stats_read = iosections["section_stats_read"]
    stats_write = iosections["section_stats_write"]

    start_middle_diffs_read.append(stats_read[0]["avg"] - (mean(stats_read[1:3])))
    start_middle_diffs_write.append(stats_write[0]["avg"] - mean(stats_write[1:3]))

    caps_middle_diffs_read.append(mean(stats_read[::3]) - mean(stats_read[1:3]))
    caps_middle_diffs_write.append(mean(stats_write[::3]) - mean(stats_write[1:3]))

    middle_end_diffs_read.append(mean(stats_read[1:3]) - stats_read[3]["avg"])
    middle_end_diffs_write.append(mean(stats_write[1:3]) - stats_write[3]["avg"])

    ratios.append(job["iosections"]["ratio_start_middle_read"])

    data_totals_read.append(job['gpfs'][GPFS_NAME]['read_bytes']['avg'])
    data_totals_write.append(job['gpfs'][GPFS_NAME]['write_bytes']['avg'])

data_totals_read = np.log2(data_totals_read)
data_totals_write = np.log2(data_totals_write)

plt.subplot(2, 1, 1)
plt.scatter(data_totals_read, start_middle_diffs_read, c='blue')
plt.scatter(data_totals_read, caps_middle_diffs_read, c='red')
plt.scatter(data_totals_read, middle_end_diffs_read, c='green')

plt.subplot(2, 1, 2)
plt.scatter(data_totals_write, start_middle_diffs_write, c='blue')
plt.scatter(data_totals_write, caps_middle_diffs_write, c='red')
plt.scatter(data_totals_write, middle_end_diffs_write, c='green')


# plt.scatter(data_totals, normalize_inf(ratios), c='purple')

# plt.hist2d(data_totals, normalize_inf(ratios), (10, 10), cmap=plt.cm.jet)
# plt.hexbin(data_totals, normalize_inf(ratios), gridsize=10, bins=None, cmap=plt.cm.jet)

# plt.hist2d(start_middle_diffs_read, middle_end_diffs_read, (20, 20), cmap=plt.cm.jet)

plt.axhline(0, color='black')
plt.show()
