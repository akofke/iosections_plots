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

results = Results(rebuild_cache=False)
plotter = plotly.offline.iplot


def to_mb(b):
    return b / 1024.0 / 1024


def st(stat, jobs=None, transform=to_mb):
    """
    Generates a data series for a single stat key, e.g. r0 or gpfs_write.
    :param jobs:
    :param stat: the key string for the stat
    :param transform: a function to apply to each data point (e.g. to_mb to convert bytes to mb)
    :return: a list of data points
    """
    _jobs = jobs if jobs else results.data
    return stat, [transform(job[stat]) if transform else job[stat] for job in _jobs]


def stdiff(stat1, stat2, jobs=None, transform=to_mb):
    """
    Generate a data series for a difference between two stats, e.g. r0 - r1
    :param stat1:
    :param stat2:
    :param transform:
    :return:
    """
    _jobs = jobs if jobs else results.data
    return f'{stat1} - {stat2}', [transform(job[stat1] - job[stat2]) if transform else job[stat1] - job[stat2] for job
                                  in _jobs]


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
    """
    :param axdata: a varargs list of tuples, with the first element of the tuple being the x axis data (created with one
    of the data-generating functions), and the second being the y axis data.
    :param logx: True if the x axis should use a log scale
    :param logy: True if the y axis should use a log scale
    """

    traces = []
    for xdata, ydata in axdata:
        xlabel, xs = xdata
        ylabel, ys = ydata

        trace = gr.Scattergl(
            x=xs,
            y=ys,
            mode="markers",
            text=[f'{j["res_id"]},{j["local_job_id"]}' for j in results.data]
        )

        traces.append(trace)

    layout = gr.Layout(
        title=f'{xlabel} vs {ylabel}',
        hovermode='closest',
        xaxis={'title': xlabel, 'type': 'log' if logx else '-'},
        yaxis={'title': ylabel, 'type': 'log' if logy else '-'}
    )

    fig = gr.Figure(data=traces, layout=layout)

    plotter(fig)


def plot3d(*axdata, sample=20000, logx=False, logy=False, logz=False):
    """

    :param axdata: A varargs list of tuples, with the first element of the tuple being the data for the x axis, then
     the y axis, then the z axis.
    :param sample:
    :param logx:
    :param logy:
    :param logz:
    :return:
    """
    # sampled_results = np.random.choice([axdata], size=sample)
    # xs = [job['r0'] for job in sampled_results]
    # ys = [job['r3'] for job in sampled_results]
    # zs = [job['gpfs_read'] for job in sampled_results]

    traces = []

    for xdata, ydata, zdata in axdata:
        xlabel, xs = xdata
        ylabel, ys = ydata
        zlabel, zs = zdata

        traces.append(
            gr.Scatter3d(
                x=xs,
                y=ys,
                z=zs,
                mode='markers',
                marker={'size': 2}
            )
        )

    layout = gr.Layout(
        scene=gr.Scene(
            xaxis=gr.XAxis(title=xlabel, type='log' if logx else '-'),
            yaxis=gr.YAxis(title=ylabel, type='log' if logy else '-'),
            zaxis=gr.ZAxis(title=zlabel, type='log' if logz else '-')
        )
    )

    fig = gr.Figure(data=traces, layout=layout)
    plotter(fig)


def generate_html(fig):
    div = plotly.offline.plot(fig, show_link=False, output_type='div')
    with open('xdmod_open.js') as js:
        tag = f'<script>{js.read()}</script>'

    with open('plot.html', 'w') as html:
        html.write(f'{div}{tag}')


def main():
    # plot_by_application()
    # plot3d()
    global plotter
    global results
    plotter = generate_html
    # scatter2d((stdiff('r0', 'w0'), stdiff('r3', 'w3')))
    # plot_by_category(
    #     "pi",
    #     partial(st, "gpfs_read"),
    #     partial(st, 'gpfs_write'),
    # )
    results.filter(lambda j: to_mb(j['gpfs_read']) > 10000000)
    print(len(results.data))
    scatter2d((stdiff("r1", "r3"), st("caps_mid_diff_read")), (stdiff("r1", "r3"), st("caps_mid_diff_write")))


if __name__ == '__main__':
    main()
