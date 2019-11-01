#!/usr/bin/env python
import datetime

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import redis
from dash.dependencies import Input, Output

from wikiutils import aggregation_interval_sec, all_timestamps, redis_url

r = redis.Redis.from_url(redis_url)


def get_layout():
    return html.Div(
        children=[
            get_dropdown(),
            dcc.Graph(id="event-rate-plot"),
            dcc.Interval(
                id="interval-component",
                interval=aggregation_interval_sec * 1000,  # in milliseconds
                n_intervals=0,
            ),
        ]
    )


def figure_data(domains):
    ret = []
    for domain in domains:
        dt_list, values = get_events(domain)
        sc = go.Bar(x=dt_list, y=values, name=domain)
        ret.append(sc)
    return ret


def get_dropdown():
    initial_value = ["en.wikipedia.org", "pt.wikipedia.org"]
    domains = [d.decode("utf8") for d in r.smembers("known_domains")]
    options = [{"value": d, "label": d} for d in domains]
    return dcc.Dropdown(options=options, multi=True, id="domains", value=initial_value)


def get_events(domain):
    timestamps = all_timestamps()
    redis_keys = [f"ev:{domain}:{ts}" for ts in timestamps]
    values = [int(v) if v else 0 for v in r.mget(redis_keys)]
    dt_list = [datetime.datetime.fromtimestamp(ts) for ts in timestamps]
    return dt_list, values


app = dash.Dash(__name__)
app.layout = get_layout()


@app.callback(
    Output(component_id="event-rate-plot", component_property="figure"),
    [
        Input(component_id="domains", component_property="value"),
        Input(component_id="interval-component", component_property="n_intervals"),
    ],
)
def update_output_div(domains, n):
    return go.Figure(
        data=figure_data(domains),
        layout=go.Layout(
            title=f"Wikipedia changes per {aggregation_interval_sec} seconds"
        ),
    )


if __name__ == "__main__":
    app.run_server(debug=True)
