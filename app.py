# Copyright 2019 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
""" Dashboard to pull and monitor alerts emitted by the Fink broker.
"""
import time
import os
import pathlib

import numpy as np
import pandas as pd

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from fink_client.consumer import AlertConsumer
from fink_client.avroUtils import write_alert, read_alert

from db.api import get_alert_monitoring_data
from db.api import update_alert_monitoring_db
from db.api import get_alert_per_topic

app = dash.Dash(
    __name__,
    meta_tags=[{
        "name": "viewport",
        "content": "width=device-width, initial-scale=1"
    }],
)

server = app.server
app.config.suppress_callback_exceptions = True

BASE_PATH = pathlib.Path(__file__).parent.resolve()

# Path to put in a configuration file
DATA_PATH = BASE_PATH.joinpath("data").resolve()

# To put in a configuration file
testmode = True # Allow to overwrite alerts and loop over a subset of inputs
maxtimeout = 1  # timeout (seconds)
mytopics = ["rrlyr", "ebwuma", "unknown"]
test_servers = "localhost:9093,localhost:9094,localhost:9095"
test_schema = "tests/test_schema.avsc"
db_path = 'db/alert-monitoring.db'

myconfig = {
    'bootstrap.servers': test_servers,
    'group_id': 'test_group'}

# Instantiate a consumer
consumer = AlertConsumer(mytopics, myconfig, schema=test_schema)

# List topics
topic_dic = consumer._consumer.list_topics().topics.keys()
topic_list = [i for i in topic_dic if i[0:2] != "__"]

def build_tabs():
    """ Build the two tabs of the dashboard.

    - Tab 1: Stream connector. Pull alerts and save them on disk. You can also
        watch number of alerts per topic received in the previous 5 minutes.
    - Tab 2: Alert explorer. Display properties of each received alert.

    Returns
    ---------
    html.Div: Div with two chidren.
    """
    return html.Div(
        id="tabs",
        className="tabs",
        children=[
            dcc.Tabs(
                id="app-tabs",
                value="tab1",
                className="custom-tabs",
                children=[
                    dcc.Tab(
                        id="stream-tab",
                        label="Stream connector",
                        value="tab1",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                    ),
                    dcc.Tab(
                        id="alert-tab",
                        label="Alert explorer",
                        value="tab2",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                    ),
                ],
            )
        ],
    )

@app.callback(
    [Output("app-content", "children")],
    [Input("app-tabs", "value")]
)
def render_tab_content(tab_switch: str) -> list:
    """ Build a tab.

    Callbacks
    ----------
    Input: takes the main `app.layout`.
    Output: builds one of the `dcc.Tabs`.

    Parameters
    ----------
    tab_switch: str
        `value` of the current `dcc.Tabs`.

    Returns
    ----------
    list of list containing `html.div`.
    """
    if tab_switch == "tab1":
        return build_tab_1()
    else:
        return build_tab_2()

def build_tab_1():
    """ Build the first tab, which is the stream connector.

    There are 2 main components:
        - Left column containing description and `next` button.
        - Right column containing the graph with streams

    TODO:
        - add div with total number of alerts per topic since the beginning
          of the database.
    """
    msg = """
    Hit the Poll button to download the next alert sent by the Fink broker.
    More description to come.
    """
    divs = [
        # Left column
        html.Div(
            id="left-column",
            className="four columns",
            children=[
                description_card(msg),
                generate_control_card_tab1()
            ] + [
                html.Div(
                    ["initial child"],
                    id="output-clientside",
                    style={"display": "none"}
                )
            ],
        ),
        # Right column
        html.Div([
            dcc.Graph(
                id='topic-bar-graph',
                figure={
                    'layout': {
                        'clickmode': 'event+select+marker',
                        'autosize': True
                    }
                }
            )], style={
            'width': '49%',
            'height': '20%',
            'display': 'inline-block',
            'padding': '0 20'}
        )
    ]
    return [divs]

def build_tab_2():
    """ Build the second tab, which is the alert explorer.

    There are 2 main components:
        - Left column containing description and dropdown menus in cascade:
            first the user choose a topic, and then an alert ID for display.
        - Right column containing the graphs with alert properties.

    TODO:
        - Add div for displaying thumbnails
        - Add div to display table of alert properties
        - Add div to display view in Simbad
    """
    msg = """
    Choose a topic, and an alert ID to see the properties of the alert.
    More description to come.
    """
    divs = [
        # Left column
        html.Div(
            id="left-column",
            className="four columns",
            children=[
                description_card(msg),
                generate_control_card_tab2()
            ] + [
                html.Div(
                    ["initial child"],
                    id="output-clientside",
                    style={"display": "none"}
                )
            ],
        ),
        # Right column (only light curve for the moment)
        html.Div([dcc.Graph(
            id='light-curve',
            figure={
                'layout': {
                    'clickmode': 'event+select',
                    "autosize": True
                }
            }
        )], style={
            'width': '49%',
            'display': 'inline-block',
            'padding': '0 20'
        })
    ]
    return [divs]

def description_card(msg: str):
    """ Construct a div containing dashboard title & descriptions.

    This title is currently common to all tabs, only the description change.

    Parameters
    ----------
    msg: str
        Message (description) to display on the left column of each tab.

    Returns
    ----------
    div: A Div containing dashboard title & descriptions.
    """
    return html.Div(
        id="description-card",
        children=[
            html.H5("Fink dashboard"),
            html.H3("Start exploring the Universe"),
            html.Div(
                id="intro",
                children=msg,
            ),
        ],
    )

def generate_control_card_tab1():
    """ Build a div that containing controls (button) for graphs.

    Currently contains:
        - Poll button to download alerts

    Returns
    ---------
    div: A Div containing controls for graphs in tab 1.
    """
    return html.Div(
        id="control-card",
        children=[
            html.Div(
                html.Button(
                    "Poll a new alert",
                    id="poll-button",
                    style={"display": "inline-block", "marginLeft": "10px"},
                    n_clicks=0,
                ),
                className="page-buttons",
                id="poll-button-div",
            ),
            html.Div(id='container-button-Poll'),
        ],
    )

def generate_control_card_tab2():
    """ Build a div that containing controls (button) for graphs.

    Currently contains:
        - dropdown menu to choose topic
        - dropdown menu to choose alert ID based on previously chosen topic.

    Returns
    ---------
    div: A Div containing controls for graphs in tab 2.
    """
    return html.Div(
        id="control-card",
        children=[
            html.P("Select Topic"),
            # Topic list
            dcc.Dropdown(
                id="topic-select",
                options=[{"label": i, "value": i} for i in topic_list],
                value=topic_list[0],
                clearable=False,
                style={'width': '100%', 'display': 'inline-block'}
            ),
            html.Br(),
            # Alert ID list
            dcc.Dropdown(
                id="alerts-dropdown",
                placeholder="Select an alert ID",
                clearable=False,
                style={'width': '200px', 'display': 'inline-block'}
            ),
            html.Br(),
            html.Div(id='container-button-timestamp'),
        ],
    )

@app.callback(
    dash.dependencies.Output('alerts-dropdown', 'options'),
    [dash.dependencies.Input('topic-select', 'value')])
def set_alert_dropdown(topic: str) -> list:
    """ According to the selected topic in `topic-select`, retrieve the
    corresponding alert ID from the monitoring database and populate the
    `alerts-dropdown` menu.

    Callbacks
    ---------
    Input: incoming topic from the `topic-select` dropdown menu
    Output: list of objectId alerts corresponding to the topic.

    Parameters
    ----------
    topic: str
        Name of a subscribed topic

    Returns
    ----------
    list: List of objectId alerts (str).
    """
    id_list = get_alert_per_topic(db_path, topic)
    return [{'label': i, 'value': i} for i in id_list]

@app.callback(
    [dash.dependencies.Output('light-curve', 'figure')],
    [dash.dependencies.Input('alerts-dropdown', 'value')])
def draw_light_curve(alert_id):
    """ Display alert light curve based on its ID.

    TODO:
        - Make meaningful plot! we currently draw random number for
          test purposes.
        - Label axes.

    Callbacks
    ----------
    Input: alert_id coming from the `alerts-dropdown` menu
    Output: Graph to display the historical light curve data of the alert.

    Parameters
    ----------
    alert_id: str
        ID of the alerts (must be unique and saved on disk).

    Returns
    ----------
    html.div: Graph data and layout based on incoming alert data.
    """
    # Load the alert from disk
    alert = read_alert(os.path.join(DATA_PATH, "{}.avro".format(alert_id)))

    # Should be replaced by alert data
    xx = np.random.rand(10)
    yy = np.random.rand(10)

    # Title of the plot
    name = alert["objectId"]
    title = alert["objectId"]

    # Update graph data
    out_graph = {'data': [
        {
            'x': xx,
            'y': yy,
            "hoverinfo": "label",
            "textinfo": "label",
            'name': name,
            'mode': 'markers',
            'marker': {'size': 12}
        }
    ], "layout": {
        "showlegend": True,
        'title': title,
        "paper_bgcolor": "white",
        "plot_bgcolor": "rgba(0,0,0,0)",
    }}
    return [out_graph]

@app.callback(
    [dash.dependencies.Output('container-button-Poll', 'children'),
     dash.dependencies.Output('topic-bar-graph', 'figure')],
    [dash.dependencies.Input('poll-button', 'n_clicks')])
def poll_alert_and_show_stream(btn1: int):
    """ This routine poll a new alert, update the graph of stream, and save
    the alert on disk.

    When the button 'Poll' is hit, we poll a new alert, save it on disk,
    and try to update the graph. Otherwise if the result of the poll is None,
    we just warn the user that no alerts were found until the timeout.

    Note this function has two outputs: 'Poll' button, and the graph.

    TODO:
        - Let the user define how far in the past he wants to go.

    Callbacks
    ----------
    Input: information that the Poll button has been hit
    Output: Display a message below the Poll button with status of the Poll
    Output: Update the graph of streams (`topic-bar-graph`)

    Parameters
    ----------
    bnt1: int
        Number of times the button 'Poll' has been clicked.

    Returns
    ----------
    out1: html.Div @ container-button-Poll
        print a message below the button `Poll` to
        inform about the request status
    out2: html.div @ `topic-bar-graph`
        Graph data and layout based on incoming alerts.
    """
    # Try to poll a new alert
    topic, alert = consumer.poll(timeout=maxtimeout)

    if alert is not None:
        # Message to print under the `Poll` button
        msg = "Alert received!"

        # Update the monitoring database
        now = time.time()
        df = pd.DataFrame({
            "objectId": [alert["objectId"]],
            "time": [now],
            "topic": topic
        })
        update_alert_monitoring_db(db_path, df)

        # Save the alert on disk for later inspection
        if testmode:
            write_alert(alert, test_schema, DATA_PATH, overwrite=True)
        else:
            write_alert(alert, test_schema, DATA_PATH, overwrite=False)
    else:
        # Message to print under the `next` button
        msg = f"no alerts received in {maxtimeout} second(s)"

    # Query the monitoring database to retrieve last entries per topic
    data = []
    bins = np.linspace(-300, 0, 30)
    try:
        now = time.time()
        df_monitoring = get_alert_monitoring_data(db_path, now - 300, now)
        for topic in topic_list:
            # Get alert times and IDs per topic
            df_topic = df_monitoring[df_monitoring["topic"] == topic]
            data_time = df_topic["time"]
            data_id = df_topic["objectId"]

            # Make an histogram for times
            counts, times = np.histogram(data_time - now, bins=bins)

            # Append plot data per topic
            data.append({
                'x': times,
                'y': counts,
                'type': 'bar',
                "hoverinfo": "label",
                "hovertext": data_id,
                "hover_data": data_id,
                'hovermode': "closest",
                "textinfo": data_id,
                'name': topic
            })
    except pd.io.sql.DatabaseError as e:
        # If something happen with the DB, plot empty data.
        print(e)
        data.append({
            'x': [],
            'y': [],
            'type': 'bar',
            "hoverinfo": "label",
            "textinfo": "label",
            'hovermode': "closest",
            'name': 'None'
        })

    # Update message below `Poll` button
    out_button = html.Div([html.Div(msg)])

    # Update graph data
    out_graph = {
        'data': data,
        "layout": {
            "showlegend": True,
            'title': "Alerts received in the last 5 minutes"
        }
    }
    return out_button, out_graph


# Top level of the app.
app.layout = html.Div(
    id="app-container",
    children=[
        # Banner
        html.Div(
            id="banner",
            className="banner",
            children=["Fink Monitor - version 0.1.0"],
        ),
        # Build the 2 tabs
        build_tabs(),
        html.Div(id="app-content")
    ],
)

# Run the server
if __name__ == "__main__":
    app.run_server(debug=True)
