from collections import defaultdict, Counter
from functools import partial

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.axes3d import Axes3D
import plotly
import plotly.graph_objs as gr
import numpy as np

from db.mongo import extract_jobs_data
from db import GPFS_NAME
from db.mysql import Results

results = Results()
plotter = plotly.offline.iplot


def to_mb(b):
    return b / 1024.0 / 1024


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


def st(stat, jobs=results.data, transform=to_mb):
    """
    Generates a data series for a single stat key, e.g. r0 or gpfs_write.
    :param stat: the key string for the stat
    :param transform: a function to apply to each data point (e.g. to_mb to convert bytes to mb)
    :return: a list of data points
    """
    return stat, [transform(job[stat]) if transform else job[stat] for job in jobs]


def stdiff(stat1, stat2, jobs=results.data, transform=to_mb):
    """
    Generate a data series for a difference between two stats, e.g. r0 - r1
    :param stat1:
    :param stat2:
    :param transform:
    :return:
    """
    return f'{stat1} - {stat2}', [transform(job[stat1] - job[stat2]) if transform else job[stat1] - job[stat2] for job in jobs]


def plot_by_category(cat_key, xfunc, yfunc, n_most_common=8, logx=False, logy=False):

    # use a defaultdict so new keys automatically start a list
    jobs_by_category = defaultdict(list)

    # sort the jobs into lists all with the same key
    for job in results.data:
        jobs_by_category[job[cat_key]].append(job)

    # count the number of jobs in each category
    category_count = Counter({cat: len(jlist) for cat, jlist in jobs_by_category.items()})

    # filter down to just the n most common categories
    jobs_by_category = {a[0]: jobs_by_category[a[0]] for a in category_count.most_common(n_most_common)}

    traces = []

    for cat, jobs in jobs_by_category.items():
        xlabel, xs = xfunc(jobs=jobs)
        ylabel, ys = yfunc(jobs=jobs)
        trace = gr.Scattergl(
            x=xs,
            y=ys,
            name=cat,
            mode='markers',
            marker={
                'size': 5
            }
        )

        traces.append(trace)

    layout = gr.Layout(
        title=f'Jobs by {cat_key}',
        xaxis={
            'title': xlabel,
            'type': 'log' if logx else '-'
        },
        yaxis={
            'title': ylabel,
            'type': 'log' if logy else '-'
        }
    )

    fig = gr.Figure(data=traces, layout=layout)
    plotter(fig)


def scatter2d(*axdata, logx=False, logy=False):
    traces = []
    for xdata, ydata in axdata:
        xlabel, xs = xdata
        ylabel, ys = ydata

        trace = gr.Scattergl(
            x=xs,
            y=ys,
            mode="markers"
        )

        traces.append(trace)

    layout = gr.Layout(
        title=f'{xlabel} vs {ylabel}',
        xaxis={'title': xlabel, 'type': 'log' if logx else '-'},
        yaxis={'title': ylabel, 'type': 'log' if logy else '-'}
    )

    fig = gr.Figure(data=traces, layout=layout)

    plotter(fig)


def plot3d():
    sampled_results = np.random.choice(results.data, size=20000)
    xs = [job['r0'] for job in sampled_results]
    ys = [job['r3'] for job in sampled_results]
    zs = [job['gpfs_read'] for job in sampled_results]

    trace = gr.Scatter3d(
        x=xs,
        y=ys,
        z=zs,
        mode='markers',
        marker={'size': 2}
    )

    data = [trace]
    layout = gr.Layout(
        scene=gr.Scene(
            xaxis=gr.XAxis(title='r0'),
            yaxis=gr.YAxis(title='r3'),
            zaxis=gr.ZAxis(title='gpfs_read')
        )
    )

    fig = gr.Figure(data=data, layout=layout)
    plotter(fig)


def main():
    # plot_by_application()
    # plot3d()
    global plotter
    plotter = plotly.offline.plot
    # scatter2d((stdiff('r0', 'w0'), stdiff('r3', 'w3')))
    results.filter(lambda j: to_mb(j['gpfs_write']) > 1000)
    plot_by_category(
        "appname",
        partial(st, 'gpfs_write', ),
        partial(st, 'caps_mid_diff_write'),
        logx=True,
    )

if __name__ == '__main__':
    main()
