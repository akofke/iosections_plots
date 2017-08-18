from collections import defaultdict, Counter
from functools import partial

import jsonpickle
import jsonpickle.ext.numpy as jnp
import matplotlib

jnp.register_handlers()
import plotly
import plotly.graph_objs as gr
import numpy as np
import scipy
from scipy import integrate, stats
from autoperiod import Autoperiod
import matplotlib.pyplot as plt

from db.mysql import Results
from db import mongo
import db

results = Results(rebuild_cache=False)
plotter = plotly.offline.iplot


def to_mb(b):
    return b / 1024.0 / 1024


def st(stat, jobs=None, transform=None):
    """
    Generates a data series for a single stat key, e.g. r0 or gpfs_write.
    :param jobs:
    :param stat: the key string for the stat
    :param transform: a function to apply to each data point (e.g. to_mb to convert bytes to mb)
    :return: a list of data points
    """
    _jobs = jobs if jobs else results.data
    return stat, [transform(job[stat]) if transform else job[stat] for job in _jobs]


def stdiff(stat1, stat2, jobs=None, transform=None):
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
    matplotlib.rcParams.update({'font.size': 7})
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

    fig, ax = plt.subplots(figsize=(4.8,4.8), dpi=800)

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
            },
            text=[f'{j["res_id"]},{j["local_job_id"]}' for j in jobs]
        )

        # ax.scatter(xs, ys, s=2, label=cat)

        traces.append(trace)

    layout = gr.Layout(
        title=f'Jobs by {cat_key}',
        xaxis={
            'title': xlabel,
            'range': [0, 1],
            'type': 'log' if logx else '-'
        },
        yaxis={
            'title': ylabel,
            'range': [-1, 1],
            'type': 'log' if logy else '-'
        }
    )

    # ax.set_xlabel("First section - Fourth Section")
    # ax.set_ylabel("Cap sections - Middle Sections")
    # # ax.set_title(f'Jobs by {cat_key}')
    # ax.legend()
    # fig.tight_layout()
    # fig.savefig('test.png', format='png', dpi=800)
    # plt.show()
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


def plot_autoperiod_areadiff_vs_variation():
    allnodes = []
    total_jobs = 0

    for cur in [mongo.extract_autoperiod_jobs(res) for res in db.RESOURCE_NAMES]:
        for job in cur:
            total_jobs += 1
            nodes = job['iosections']['node_periods']
            for nodename, rw in nodes.items():
                for s in ("read", "write"):
                    sect_stats = job['iosections'][f'section_stats_{s}']
                    key = lambda stat: stat['avg']
                    maxavg = max(sect_stats, key=key)['avg']
                    minavg = min(sect_stats, key=key)['avg']

                    var = (maxavg - minavg) / maxavg if maxavg != 0 else 0
                    node = {
                        "rw": s,
                        "resource_id": job['acct']['resource_id'],
                        "local_job_id": job['acct']['local_job_id'],
                        "walltime": job['acct']['end_time'] - job['acct']['start_time'],
                        "cputime": (job['acct']['end_time'] - job['acct']['start_time']) * job['acct']['ncpus'],
                        "nodename": nodename,
                        "autoperiod": jsonpickle.decode(rw[s]),
                        "var": var
                    }
                    if node['autoperiod'].period is not None and node["var"] <= 1 and node['autoperiod'].period > 0:
                        allnodes.append(node)

    normalized_period_scores = []
    on_per_vars = []
    periods = []
    walltimes = []
    cputimes = []
    colors = []
    print(f"Total: {total_jobs}")
    print(len(allnodes))
    for node in allnodes:
        a = node['autoperiod']
        on_per_area, off_per_area = a.period_area()
        on_per_blocks, off_per_blocks = a.period_blocks()
        on_per_block_areas = np.array([scipy.integrate.trapz(arr[1], x=arr[0]) for arr in on_per_blocks])
        # off_per_block_areas = np.array([scipy.integrate.trapz(arr[1], x=arr[0]) for arr in off_per_blocks])
        on_per_var = scipy.stats.variation(on_per_block_areas)
        # off_per_var = scipy.stats.variation(off_per_block_areas)

        normalized_period_scores.append((on_per_area - off_per_area) / (on_per_area + off_per_area))
        # ys.append(scipy.stats.variation(a.values))
        on_per_vars.append(on_per_var)
        colors.append(node['var'])
        periods.append(a.period)
        walltimes.append(node['walltime'])
        cputimes.append(node['cputime'])

    trace_scatter = gr.Scattergl(
        x=normalized_period_scores,
        y=on_per_vars,
        mode='markers',
        marker={
            'size': 3,
            'color': colors,
            'colorbar': gr.ColorBar(title='Variability of time sections'),
            'colorscale': 'Bluered'
        },
        text=[f'{n["resource_id"]},{n["local_job_id"]},p={n["autoperiod"].period}' for n in allnodes])

    layout_scatter = gr.Layout(
        xaxis={'title': "Difference of on-period and off-period area"},
        yaxis={'title': "Coefficient of variation of on-period area"}
    )

    fig = gr.Figure(data=[trace_scatter], layout=layout_scatter)
    plotter(fig, filename='plot-scatter.html')


    trace_hist_periods = gr.Histogram(
        x=periods
    )

    layout_hist_periods = gr.Layout(
        xaxis={'title': "Job periods"},
        yaxis={'title': "Count"}
    )

    fig = gr.Figure(data=[trace_hist_periods], layout=layout_hist_periods)
    plotter(fig, filename='plot-hist-periods.html')

    trace_2dhist = gr.Histogram2d(
        x=walltimes,
        y=periods
    )
    #
    # trace2 = gr.Scattergl(
    #     x=walltimes,
    #     y=periods,
    #     mode='markers',
    #     marker={"size": 3},
    #     text=[f'{n["resource_id"]},{n["local_job_id"]},{n["nodename"]}' for n in allnodes]
    # )

    layout_2dhist = gr.Layout(
        xaxis={'title': "Job wall time"},
        yaxis={'title': 'Job period'}
    )

    fig = gr.Figure(data=[trace_2dhist], layout=layout_2dhist)
    plotter(fig, filename='plot-period-2dhist.html')

    trace_period_walltime_scatter = gr.Scattergl(
        x=walltimes,
        y=periods,
        mode='markers',
        marker={
            'size': 3,
            'color': colors,
            'colorbar': gr.ColorBar(title='Variability of time sections'),
            'colorscale': 'Bluered'
        },
        text=[f'{n["resource_id"]},{n["local_job_id"]},p={n["autoperiod"].period}' for n in allnodes]
    )

    layout_period_walltime_scatter = gr.Layout(
        xaxis={'title': "Job wall time"},
        yaxis={'title': 'Job period'}
    )

    fig = gr.Figure(data=[trace_period_walltime_scatter], layout=layout_period_walltime_scatter)
    plotter(fig, filename='plot-period-walltime-scatter.html')

    cputime_hist_count, cputime_hist_bins = np.histogram(periods, bins=40, weights=cputimes)
    trace_cputime_hist = gr.Bar(
        x=cputime_hist_bins,
        y=cputime_hist_count
    )

    layout_cputime_hist = gr.Layout(
        xaxis={'title': 'Period bin'},
        yaxis={'title': 'Total cputime'}
    )

    fig = gr.Figure(data=[trace_cputime_hist], layout=layout_cputime_hist)
    plotter(fig, filename='plot-cputime-hist.html')

def autoperiod_plots(jobs):
    pass



def generate_html(fig, filename="plot.html"):
    div = plotly.offline.plot(fig, show_link=False, output_type='div')
    with open('xdmod_open.js') as js:
        tag = f'<script>{js.read()}</script>'

    with open(filename, 'w') as html:
        html.write(f'{div}{tag}')


def main():
    # results.rebuild_cache()

    global plotter
    plotter = generate_html
    # # scatter2d((stdiff('r0', 'w0'), stdiff('r3', 'w3')))
    # # plot_by_category(
    # #     "pi",
    # #     partial(st, "gpfs_read"),
    # #     partial(st, 'gpfs_write'),
    # # )
    # # results.filter(lambda j: to_mb(j['gpfs_read']) > 1000)
    # # results.filter(lambda j: j["caps_mid_diff_read"] > 0)
    # print(len(results.data))
    # # scatter2d((stdiff("r0", "r3"), st("caps_mid_diff_read")))
    # # scatter2d((stdiff("w0", "w3"), st("caps_mid_diff_write")))
    #

    results.filter(lambda j: j['period_r'] is not None)
    plot_by_category(
        "appname",
        partial(st, 'read_variability'),
        partial(st, "period_score_r"),
        n_most_common=8
    )
    # scatter2d((st("r0"), st("w3")))

    # plot_autoperiod_areadiff_vs_variation()


if __name__ == '__main__':
    main()
