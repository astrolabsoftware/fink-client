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
""" Dashboard to pull and explore alerts emitted by the Fink broker.
"""
import time
import datetime
import os
import pathlib
import gzip
import io

import numpy as np
import pandas as pd
from astropy.io import fits

import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

import visdcc

from fink_client.consumer import AlertConsumer
from fink_client.avroUtils import write_alert, read_alert
import fink_client.fink_client_conf as fcc

from db.api import get_alert_monitoring_data
from db.api import update_alert_monitoring_db
from db.api import get_information_per_topic

external_scripts = [
    'http://code.jquery.com/jquery-1.9.1.min.js',
    'http://aladin.u-strasbg.fr/AladinLite/api/v2/latest/aladin.min.js'
]

external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    'http://aladin.u-strasbg.fr/AladinLite/api/v2/latest/aladin.min.css'
]

app = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    external_scripts=external_scripts,
    meta_tags=[{
        "name": "viewport",
        "content": "width=device-width, initial-scale=1"
    }],
)

app.config.suppress_callback_exceptions = True

BASE_PATH = pathlib.Path(__file__).parent.resolve()

# Path to put in a configuration file
DATA_PATH = BASE_PATH.joinpath("data").resolve()

myconfig = {
    "username": fcc.username,
    "password": fcc.password,
    'bootstrap.servers': fcc.servers,
    'group_id': fcc.group_id}

# Instantiate a consumer
consumer = AlertConsumer(fcc.mytopics, myconfig, schema=fcc.schema)

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
    return html.Div([
        dbc.Tabs(
            [
                dbc.Tab(label="Stream connector", tab_id="tab-1"),
                dbc.Tab(label="Alert explorer", tab_id="tab-2"),
            ],
            id="tabs",
            active_tab="tab-1",
        ),
        html.Div(id="content"),
    ])

@app.callback(
    [Output("app-content", "children")],
    [Input("tabs", "active_tab")]
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
    if tab_switch == "tab-1":
        return build_tab_1()
    else:
        return build_tab_2()

def build_tab_1():
    """ Build the first tab, which is the stream connector.

    There are 2 main components:
        - Left column containing description and `next` button.
        - Right column containing the graph with streams
    """
    msg = """
    Hit the Poll button to download new alerts sent by the Fink broker.
    Alternatively, you can go to the second Tab to check received alerts.
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
    """
    msg = """
    Choose a topic, and an alert ID to see the properties of the alert.
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
        html.Div(
            [make_item(2), make_item(1), make_item(3)], className="accordion"),
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
            html.H3("Explore the transient sky"),
            html.Div(msg),
            html.Br()
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
                    "Poll new alerts",
                    id="poll-button",
                    style={"display": "inline-block", "marginLeft": "10px"},
                    n_clicks=0,
                ),
                className="page-buttons",
                id="poll-button-div",
            ), html.Div(id='container-button-Poll')] + [
                i for i in generate_table()
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
            html.P("Select Alert"),
            dcc.Dropdown(
                id="alerts-dropdown",
                placeholder="Select an alert ID",
                clearable=False,
                style={'width': '300px', 'display': 'inline-block'}
            ),
            html.Br(),
            html.Div(id='container-button-timestamp'),
            html.Div([visdcc.Run_js(id='aladin-lite-div')], style={
                'width': '300px',
                'height': '300px'})
        ],
    )

@app.callback(
    Output('aladin-lite-div', 'run'),
    [Input('alerts-dropdown', 'value')])
def integrate_aladin_lite(alert_id):
    """ Integrate aladin light in the 2nd Tab of the dashboard.

    the default parameters are:
        * PanSTARRS colors
        * FoV = 0.02 deg
        * SIMBAD catalig overlayed.

    Callbacks
    ----------
    Input: takes the alert ID
    Output: Display a sky image around the alert position from aladin.

    Parameters
    ----------
    alert_id: str
        ID of the alert
    """
    default_img = ""
    if alert_id:
        alert = read_alert(os.path.join(DATA_PATH, "{}.avro".format(alert_id)))

        # Coordinate of the current alert
        ra0 = extract_field(alert, 'ra')[-1]
        dec0 = extract_field(alert, 'dec')[-1]

        # Javascript. Note the use {{}} for dictionary
        img = """
        var aladin = A.aladin('#aladin-lite-div',
                  {{
                    survey: 'P/PanSTARRS/DR1/color/z/zg/g',
                    fov: 0.02,
                    target: '{} {}',
                    reticleColor: '#ff89ff',
                    reticleSize: 64
        }});
        var cat = 'https://axel.u-strasbg.fr/HiPSCatService/Simbad';
        var hips = A.catalogHiPS(cat, {{onClick: 'showTable', name: 'Simbad'}});
        aladin.addCatalog(hips);
        """.format(ra0, dec0)

        # img cannot be executed directly because of formatting
        # We split line-by-line and remove comments
        img_to_show = [i for i in img.split('\n') if '// ' not in i]

        return " ".join(img_to_show)

    return default_img

def generate_table():
    """ Generate statitics table for received alerts in the monitoring db.

    Returns
    ---------
    div1: A Div containing the title
    div2: A Div containing the table
    """
    # Grab all alerts from the db
    now = time.time()
    df_monitoring = get_alert_monitoring_data(fcc.db_path, 0, now)

    if df_monitoring.empty:
        msg = "Empty alert database - keep polling!"
        return [html.H4(msg)]

    # Count how many alerts received per topic
    df = df_monitoring\
        .groupby(by="topic", as_index=False)\
        .count()[['topic', 'objectId']]\
        .rename({"objectId": "count"}, axis='columns')

    # for each topic, grab the last time an alert has been received
    df_last_entry = df_monitoring\
        .groupby(by="topic", as_index=False)\
        .aggregate(max)\
        .rename({"time": "last received"}, axis='columns')

    # Human readable time
    df["last received"] = df_last_entry["last received"]\
        .map(lambda x: time.ctime(x))

    # first time an alert has been received
    starting_time = time.ctime(df_monitoring["time"].min())

    return [html.H4(f"Statistics since {starting_time}"), html.Table(
        # Header
        [html.Tr([html.Th(col) for col in df.columns])] +
        # Body
        [html.Tr([
            html.Td(df.iloc[i][col]) for col in df.columns
        ]) for i in range(len(df))]
    )]

def extract_history(history_list: list, field: str) -> list:
    """Extract the historical measurements contained in the alerts
    for the parameter `field`.

    Parameters
    ----------
    history_list: list of dict
        List of dictionary from alert['prv_candidates'].
    field: str
        The field name for which you want to extract the data. It must be
        a key of elements of history_list (alert['prv_candidates'])

    Returns
    ----------
    measurement: list
        List of all the `field` measurements contained in the alerts.
    """
    try:
        measurement = [obs[field] for obs in history_list]
    except KeyError as e:
        print('{} not in history data')
        measurement = [None] * len(history_list)

    return measurement

def extract_field(alert: dict, field: str) -> np.array:
    """ Concatenate current and historical observation data for a given field.

    Parameters
    ----------
    alert: dict
        Dictionnary containing alert data
    field: str
        Name of the field to extract.

    Returns
    ----------
    data: np.array
        List containing previous measurements and current measurement at the
        end. If `field` is not in `prv_candidates fields, data will be
        [None, None, ..., alert['candidate'][field]].
    """
    data = np.concatenate(
        [
            [alert["candidate"][field]],
            extract_history(alert['prv_candidates'], field)
        ]
    )
    return data

def readstamp(alert: dict, field: str) -> np.array:
    """ Read the stamp data inside an alert.

    Parameters
    ----------
    alert: dictionary
        dictionary containing alert data
    field: string
        Name of the stamps: cutoutScience, cutoutTemplate, cutoutDifference

    Returns
    ----------
    data: np.array
        2D array containing image data
    """
    stamp = alert[field]["stampData"]
    with gzip.open(io.BytesIO(stamp), 'rb') as f:
        with fits.open(io.BytesIO(f.read())) as hdul:
            data = hdul[0].data
    return data

def make_item(i: int):
    """ Build the data in the accordion in the 2nd tab.

    Parameters
    ----------
    i: int
        Item number in the accordion

    Returns
    ----------
    dbc.Card
        The data for the item to display.
    """
    to_plot = [
        # Right column (only light curve for the moment)
        html.Div([dcc.Dropdown(
            id="field-dropdown",
            placeholder="Select a field",
            clearable=False,
            style={'width': '250px', 'display': 'inline-block'}
        ), dcc.Graph(
            id='light-curve',
            figure={
                'layout': {
                    'clickmode': 'event+select',
                    "autosize": True
                }
            }, style={
                'width': '130vh',
                'display': 'inline-block',
                'padding': '0 20'
            }
        )]),
        html.Div([
            html.Div([
                html.Div([dcc.Graph(
                    id='science-stamps',
                    figure={
                        'layout': {
                            'clickmode': 'event+select'
                        }
                    },
                    style={
                        'width': '300px',
                        'height': '300px',
                        'display': 'inline-block',
                        'padding': '0 20'
                    })
                ])
            ]),
            html.Div([
                html.Div([
                    html.Div([dcc.Graph(
                        id='template-stamps',
                        figure={
                            'layout': {
                                'clickmode': 'event+select',
                                "autosize": True
                            }
                        }, style={
                            'width': '300px',
                            'height': '300px',
                            'display': 'inline-block',
                            'padding': '0 20'
                        }
                    )])
                ])
            ]),
            html.Div([
                html.Div([
                    html.Div([dcc.Graph(
                        id='difference-stamps',
                        figure={
                            'layout': {
                                'clickmode': 'event+select',
                                "autosize": True
                            }
                        },
                        style={
                            'width': '300px',
                            'height': '300px',
                            'display': 'inline-block',
                            'padding': '0 20'
                        })
                    ])
                ])
            ])
        ], style={'columnCount': 3}),
        # dbc.CardBody(f"This is the content of group {i}..."),
        html.Div(dash_table.DataTable(
            id='candidate-table',
            style_table={
                'maxHeight': '300px',
                'overflowY': 'scroll',
                'border': 'thin lightgrey solid'
            },
            fixed_rows={'headers': True, 'data': 0},
            style_cell={'width': '150px', 'textAlign': 'center'}
        ))
    ]
    names = ["Data curves", "Stamps", "Alert properties"]
    return dbc.Card(
        [
            dbc.CardHeader(
                html.H2(
                    dbc.Button(
                        "{}".format(names[i - 1]),
                        color="light",
                        block=True,
                        size="lg",
                        id=f"group-{i}-toggle",
                    )
                )
            ),
            dbc.Collapse(
                html.Div([
                    to_plot[i - 1]
                ]),
                is_open=True if i == 2 else False,
                id=f"collapse-{i}",
            ),
        ]
    )

@app.callback(
    [Output(f"collapse-{i}", "is_open") for i in range(1, 4)],
    [Input(f"group-{i}-toggle", "n_clicks") for i in range(1, 4)],
    [State(f"collapse-{i}", "is_open") for i in range(1, 4)],
)
def toggle_accordion(n1, n2, n3, is_open1, is_open2, is_open3):
    """ Build the accordion in the 2nd tab.
    """
    ctx = dash.callback_context

    if not ctx.triggered:
        return ""
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "group-1-toggle" and n1:
        return not is_open1, is_open2, is_open3
    elif button_id == "group-2-toggle" and n2:
        return is_open1, not is_open2, is_open3
    elif button_id == "group-3-toggle" and n3:
        return is_open1, is_open2, not is_open3
    return False, False, False

@app.callback(
    dash.dependencies.Output('alerts-dropdown', 'options'),
    [dash.dependencies.Input('topic-select', 'value')])
def set_alert_dropdown(topic: str) -> list:
    """ According to the selected topic in `topic-select`, retrieve the
    corresponding alert ID and timestamp from the monitoring database and
    populate the `alerts-dropdown` menu.

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
        list: List of dicts with objectId and timestamp alerts (str).
    """
    id_list = get_information_per_topic(fcc.db_path, topic, 'objectId')
    ts_list = get_information_per_topic(fcc.db_path, topic, 'timestamp')
    return [
        {'label': '{}:'.format(id) + ts, 'value': id}
        for ts, id in zip(ts_list, id_list)
    ]

@app.callback(
    dash.dependencies.Output('field-dropdown', 'options'),
    [dash.dependencies.Input('alerts-dropdown', 'value')])
def set_field_dropdown_xaxis(alert_id: str) -> list:
    """ According to the alert_id, retrieve the corresponding alert fields
    and populate the `field-dropdown` menu.

    Fields with current+historical measurements will have a star to their
    name (e.g. magpsd*, jd*, but cdsxmatch).

    Callbacks
    ---------
    Input: incoming alert_id from the `alerts-dropdown` menu
    Output: list of available alert fields.

    Parameters
    ----------
    topic: str
        Name of a subscribed topic

    Returns
    ----------
    list: List of objectId alerts (str).
    """
    alert = read_alert(os.path.join(DATA_PATH, "{}.avro".format(alert_id)))

    # Sort the prv_candidates keys
    keys = list(alert['candidate'].keys())
    hist_keys = list(alert['prv_candidates'][0].keys())
    keys = [i + "*" if i in list(hist_keys) else i for i in keys]
    keys.sort()

    return [{'label': i, 'value': i} for i in keys]

@app.callback(
    [dash.dependencies.Output('light-curve', 'figure'),
     dash.dependencies.Output('science-stamps', 'figure'),
     dash.dependencies.Output('template-stamps', 'figure'),
     dash.dependencies.Output('difference-stamps', 'figure')],
    [dash.dependencies.Input('alerts-dropdown', 'value'),
     dash.dependencies.Input('field-dropdown', 'value')])
def draw_curve(alert_id, field_name):
    """ Display alert data and stamps based on its ID.

    By default, the data curve is the light curve (magpsd vs jd).

    Callbacks
    ----------
    Input: alert_id coming from the `alerts-dropdown` menu
    Input: field_name coming from the `field-dropdown` menu
    Output: Graph to display the historical light curve data of the alert.
    Output: stamps (Science, Template, Difference)

    Parameters
    ----------
    alert_id: str
        ID of the alerts (must be unique and saved on disk).
    field_name: str
        Name of the alert field to plot (default is None).

    Returns
    ----------
    html.div: Graph data and layout based on incoming alert data.
    """
    # Remove tag for data with history (see set_field_dropdown_xaxis)
    if field_name is not None and field_name.endswith('*'):
        field_name = field_name[:-1]

    # Load the alert from disk
    alert = read_alert(os.path.join(DATA_PATH, "{}.avro".format(alert_id)))

    # Extract relevant alert data to compute light-curve
    if field_name is None or field_name == 'magpsf':
        flux = extract_field(alert, "magpsf")
        upper = extract_field(alert, "diffmaglim")
        sig = extract_field(alert, "sigmapsf")
        field_name = 'magpsf'
    else:
        flux = extract_field(alert, field_name)
        upper = np.zeros_like(flux)
        sig = np.zeros_like(flux)

    jd = extract_field(alert, "jd")
    fid = extract_field(alert, "fid")
    pid = extract_field(alert, "pid")

    # Bands and dates
    filter_color = {1: '#1f77b4', 2: '#ff7f0e', 3: '#2ca02c'}
    # [
    #     '#1f77b4',  # muted blue
    #     '#ff7f0e',  # safety orange
    #     '#2ca02c',  # cooked asparagus green
    #     '#d62728',  # brick red
    #     '#9467bd',  # muted purple
    #     '#8c564b',  # chestnut brown
    #     '#e377c2',  # raspberry yogurt pink
    #     '#7f7f7f',  # middle gray
    #     '#bcbd22',  # curry yellow-green
    #     '#17becf'   # blue-teal
    # ]
    filter_name = {1: 'g band', 2: 'r band', 3: 'i band'}
    dates = np.array([i - jd[0] for i in jd])

    # Title of the plot (alert ID)
    title = alert["objectId"]

    # loop over filters
    data = []
    for filt in filter_color.keys():
        # Values corresponding to the filter
        mask = np.where(fid == filt)[0]

        # detection vs upper limit
        symbols = [
            "circle" if i is not None else "triangle-down" for i in sig[mask]]

        # y data
        ydata = np.array([
            flux[mask][i] if sig[mask][i] is not None
            else upper[mask][i] for i in range(len(mask))
        ])

        # Data to plot
        data.append(
            {
                'x': dates[mask],
                'y': ydata,
                'error_y': {
                    'type': 'data',
                    'array': sig[mask],
                    'visible': True,
                    'color': filter_color[filt]
                },
                "hoverinfo": "label",
                "hovertext": ["pid: {}".format(i) for i in pid[mask]],
                "textinfo": "label",
                'name': filter_name[filt],
                'mode': 'markers',
                'marker': {
                    'size': 12,
                    'color': filter_color[filt],
                    'symbol': symbols}
            }
        )

    # Update graph data for light-curve
    yaxis = {'title': field_name}
    if field_name == 'magpsf':
        yaxis['autorange'] = 'reversed'
    out_light_curve = {'data': data, "layout": {
        "showlegend": False,
        'yaxis': yaxis,
        'xaxis': {
            'title': 'Days to candidate'
        },
        'title': title,
        "paper_bgcolor": "white",
        "plot_bgcolor": "rgba(0,0,0,0)",
    }}

    # Update graph data for stamps
    out_stamps = []
    for field in ['Science', 'Template', 'Difference']:
        data = readstamp(alert, 'cutout{}'.format(field))
        x, y = np.meshgrid(range(len(data)), range(len(data)))

        out_stamps.append({'data': [
            {
                'x': x.flatten(),
                'y': y.flatten(),
                'z': data.flatten(),
                'type': "heatmap",
                'colorscale': "Cividis",
                "hoverinfo": "label",
                "textinfo": "label",
                'name': field
            }
        ], "layout": {
            'title': field,
            "paper_bgcolor": "white",
            "plot_bgcolor": "rgba(0,0,0,0)",
        }})

    return [out_light_curve] + out_stamps

@app.callback(
    [dash.dependencies.Output('container-button-Poll', 'children'),
     dash.dependencies.Output('topic-bar-graph', 'figure')],
    [dash.dependencies.Input('poll-button', 'n_clicks')])
def poll_alert_and_show_stream(btn1: int):
    """ This routine polls published alerts, update the graph of stream,
    and save the alert on disk.

    When the button 'Poll' is hit, we poll all new alerts, save them on disk,
    and try to update the graph. Otherwise if the result of the poll is None,
    we just warn the user that no alerts were found until the timeout.

    Note this function has two outputs: 'Poll' button, and the graph.

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
    maxtimeout = fcc.maxtimeout
    overwrite = True if fcc.testmode else False
    # Try to poll new alerts
    is_alert = True
    n_alerts = 0
    while is_alert:
        topic, alert = consumer.poll(timeout=maxtimeout)

        if alert is not None:
            n_alerts += 1
            # Message to print under the `Poll` button
            msg = "Alert received!"

            # Update the monitoring database
            now = time.time()
            df = pd.DataFrame({
                "objectId": [alert["objectId"]],
                "time": [now],
                "topic": topic,
                "timestamp": [alert["timestamp"]],
            })
            update_alert_monitoring_db(fcc.db_path, df)

            # Save the alert on disk for later inspection
            write_alert(alert, fcc.schema, DATA_PATH, overwrite=overwrite)
        else:
            # Message to print under the `next` button
            stop = "{}".format(
                datetime.datetime.now().strftime('%H:%M:%S'))
            five_minutes_ago = (
                datetime.datetime.now() - datetime.timedelta(minutes=5)
            ).strftime('%H:%M:%S')

            if n_alerts == 0:
                msg = f"No alerts received (timeout: {maxtimeout} seconds)"
            else:
                msg = f"{n_alerts} new alerts received"
            is_alert = False

    # Query the monitoring database to retrieve last entries per topic
    data = []
    bins = np.linspace(-300, 0, 30)
    # Computing the time again... need to coordinate times better
    now = time.time()
    df_monitoring = get_alert_monitoring_data(fcc.db_path, now - 300, now)
    for topic in topic_list:
        # Get alert times and IDs per topic
        if df_monitoring.empty:
            # If something happen with the DB, plot empty data.
            data.append({
                'x': [],
                'y': [],
                'type': 'bar',
                "hoverinfo": "label",
                "textinfo": "label",
                'hovermode': "closest",
                'name': 'None'
            })
        else:
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

    # Update message below `Poll` button
    out_button = html.Div([html.Div(msg)])

    # Update graph data
    title_text = [
        "Alerts received in the last 5 minutes", f"{five_minutes_ago} - {stop}"]
    title = '<br>'.join(title_text)
    out_graph = {
        'data': data,
        "layout": {
            "showlegend": True,
            'title': title
        }
    }
    return out_button, out_graph

@app.callback(
    [dash.dependencies.Output('candidate-table', 'data'),
     dash.dependencies.Output('candidate-table', 'columns')],
    [dash.dependencies.Input('alerts-dropdown', 'value')]
)
def display_candidate(alert_id):
    """ Generate statitics table for received alerts.

    Parameters
    ----------
    alert_id: str
        Alert ID from alerts-dropdown

    Returns
    ---------
    dict: dictionary from the alert pandas dataframe
    columns: list of dictionaries with column names.
    """
    # Load the alert from disk
    alert = read_alert(os.path.join(DATA_PATH, "{}.avro".format(alert_id)))

    # Load meta data first
    to_avoid = [
        'candidate', 'prv_candidates',
        'cutoutScience', 'cutoutTemplate', 'cutoutDifference'
    ]
    candidate = {k: [v] for k, v in alert.items() if k not in to_avoid}

    # Load data from current measurement
    candidate.update({k: [v] for k, v in alert['candidate'].items()})

    # formatting
    df = pd.DataFrame.from_dict(candidate).T
    df = df.reset_index(drop=False)

    # Column names
    df.columns = ['field', 'value']
    columns = [{"name": str(i), 'id': str(i)} for i in df.columns]

    return df.to_dict('rows'), columns


# Top level of the app.
app.layout = html.Div(
    id="app-container",
    children=[
        # Build the 2 tabs
        build_tabs(),
        html.Div(id="app-content")
    ],
)

# Run the server
if __name__ == "__main__":
    app.run_server(debug=True)
