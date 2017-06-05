from collections import defaultdict, Counter

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.axes3d import Axes3D
import numpy as np

from db.mongo import extract_jobs_data
from db import GPFS_NAME
from db.mysql import get_results


def normalize_inf(ratios):
    rmax = max(filter(lambda x: x != float("inf"), ratios))
    return [i if i != float("inf") else rmax for i in ratios]


def mean(lst):
    return sum(i for i in lst) / len(lst)


def generate_plots():
    ratios = []

    data_totals_read = []
    data_totals_write = []

    start_middle_diffs_read = []
    start_middle_diffs_write = []

    caps_middle_diffs_read = []
    caps_middle_diffs_write = []

    end_middle_diffs_read = []
    end_middle_diffs_write = []

    for job in extract_jobs_data("resource_13"):
        if 100 * 1024 * 1024 > job['gpfs'][GPFS_NAME]['read_bytes']['avg']:
            continue
        # if job['gpfs'][GPFS_NAME]['read_bytes']['avg'] > 2**33:
        #     continue

        iosections = job["iosections"]
        stats_read = iosections["section_stats_read"]
        stats_write = iosections["section_stats_write"]

        start_middle_diffs_read.append(stats_read[0]["avg"] - (mean(stats_read[1:3])))
        start_middle_diffs_write.append(stats_write[0]["avg"] - mean(stats_write[1:3]))

        caps_middle_diffs_read.append(mean(stats_read[::3]) - mean(stats_read[1:3]))
        caps_middle_diffs_write.append(mean(stats_write[::3]) - mean(stats_write[1:3]))
        end_middle = mean(stats_read[1:3]) - stats_read[3]["avg"]
        if end_middle < -4e8:
            print(job)
        end_middle_diffs_read.append(-1.0 * end_middle)
        end_middle_diffs_write.append(mean(stats_write[1:3]) - stats_write[3]["avg"])

        ratios.append(job["iosections"]["ratio_start_middle_read"])

        data_totals_read.append(job['gpfs'][GPFS_NAME]['read_bytes']['avg'])
        data_totals_write.append(job['gpfs'][GPFS_NAME]['write_bytes']['avg'])

    data_totals_read = np.log2(data_totals_read)
    data_totals_write = np.log2(data_totals_write)

    plt.subplot(2, 1, 1)
    plt.title("Data Read")
    plt.scatter(data_totals_read, start_middle_diffs_read, c='blue', alpha=0.5, s=3)
    plt.scatter(data_totals_read, caps_middle_diffs_read, c='red', alpha=0.5, s=3)
    plt.scatter(data_totals_read, end_middle_diffs_read, c='green', alpha=0.5, s=3)
    plt.legend(['start - middle', 'caps - middle', 'end - middle'])
    plt.axhline(0, color='black')

    plt.subplot(2, 1, 2)
    plt.title("Data Written")
    plt.scatter(data_totals_write, start_middle_diffs_write, c='blue', alpha=0.5, s=3)
    plt.scatter(data_totals_write, caps_middle_diffs_write, c='red', alpha=0.5, s=3)
    plt.scatter(data_totals_write, end_middle_diffs_write, c='green', alpha=0.5, s=3)
    plt.legend(['start - middle', 'caps - middle', 'middle - end'])
    plt.axhline(0, color='black')

    # plt.hist2d(start_middle_diffs_read, end_middle_diffs_write, (100, 100), cmap=plt.cm.jet)



    # plt.scatter(data_totals, normalize_inf(ratios), c='purple')

    # plt.hist2d(data_totals, normalize_inf(ratios), (10, 10), cmap=plt.cm.jet)
    # plt.hexbin(data_totals, normalize_inf(ratios), gridsize=10, bins=None, cmap=plt.cm.jet)

    # plt.hist2d(start_middle_diffs_read, middle_end_diffs_read, (20, 20), cmap=plt.cm.jet)

    plt.show()


def plot_by_application():
    results = get_results()

    jobs_by_app = defaultdict(list)
    jobs_by_pi = defaultdict(list)
    for job in results:
        appname = job["appname"]
        jobs_by_app[appname].append(job)
        jobs_by_pi[job['pi']].append(job)

    apps_count = Counter({app: len(jlist) for app, jlist in jobs_by_app.items()})
    pi_count = Counter({app: len(jlist) for app, jlist in jobs_by_pi.items()})

    jobs_by_app = {a[0]: jobs_by_app[a[0]] for a in apps_count.most_common(9)}
    jobs_by_pi = {a[0]: jobs_by_pi[a[0]] for a in pi_count.most_common(9)}

    cmap = plt.cm.get_cmap('hsv', 9)
    legend = []
    jobsmap = jobs_by_app
    for i, (key, jobs) in enumerate(jobsmap.items()):
        xs = []
        ys = []
        for job in jobs:
            # diff = job['r0'] - job['r1']
            diff = mean((job['r0'], job['r3'])) - mean((job['r1'], job['r2']))
            if abs(diff) > 0.25e8:
                continue
            xs.append(job['gpfs_read'])
            ys.append(diff)
        xs = np.log2(xs)
        ys = [y / 1024 ** 2 for y in ys]
        color = plt.cm.jet(1. * i / (len(jobsmap) - 1)) if key != "uncategorized" else 'black'
        plt.scatter(xs, ys, c=color, alpha=0.7, s=3)
        legend.append(key)

    plt.legend(legend)
    plt.show()


def plot3d():
    results = get_results()

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    xs = [job['r0'] for job in results]
    ys = [job['r3'] for job in results]
    zs = [job['gpfs_read'] for job in results]
    print(len(xs))

    ax.scatter(xs, ys, zs)
    ax.set_xlabel('Section 1 b/s read')
    ax.set_ylabel('Section 4 b/s read')
    ax.set_zlabel('Total bytes read')
    plt.show()


def main():
    plot_by_application()
    plot3d()
    # for res in RESOURCE_NAMES:
    #     write_csv(extract_jobs_data(res), res)


if __name__ == '__main__':
    main()
