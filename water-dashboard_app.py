import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import numpy as np
from dash import Dash, Input, Output, dcc, html, ctx
from sqlalchemy import create_engine, text
import dash_loading_spinners as dls
import plotly.graph_objects as go
import dash_auth

USER_PASSWORD_MAPPING = {"admin": "admin"}

engine = create_engine('postgresql://shiny:shiny@10.10.100.20/tuwien_zentrale')

def retrieve_tables():
    sql_query = "SELECT * FROM information_schema.tables WHERE table_schema not in ('pg_catalog', 'information_schema') and table_type = 'BASE TABLE'"
    with engine.connect() as connection:
        data_temp = connection.execute(text(sql_query))
        data = pd.DataFrame(data_temp.fetchall())
        data.columns = data_temp.keys()
        connection.commit()
    stringValA = 'tuw_'
    stringValB = '_md:'
    stringValC = 'my_dictionary'
    stringValD = 'toc_5mm_eq'
    table_list = list(data['table_name'])
    table_list = [ x for x in table_list if stringValA not in x and stringValB not in x and stringValC not in x and stringValD not in x ]
    project_tables_dict = extract_project(sorted(table_list))
    return project_tables_dict    

def extract_project(table_list):
    project_tables_dict = {}
    for table in table_list:
        if table.split('__')[0] in project_tables_dict:
            temp_list = []
            if isinstance(project_tables_dict[table.split('__')[0]],list):
                project_tables_dict[table.split('__')[0]].append(table.split('__')[1])
        else:
            project_tables_dict[table.split('__')[0]] = [table.split('__')[1]]
    return project_tables_dict

parameter_tables = retrieve_tables()
global_data = {"parameter-1": None, "parameter-2": None, "parameter-3": None, "parameter-4": None}
global_date = {"start-date": None, "end-date": None}

def get_parameters(parameter):
    sql_query = "SELECT * FROM "+parameter+" LIMIT 1"
    with engine.connect() as connection:
        data_temp = connection.execute(text(sql_query))
        data = pd.DataFrame(data_temp.fetchall())
        data.columns = data_temp.keys()
        connection.commit()
    return list(data.columns.values)

def retrieve_data(project, parameter, start_date, end_date):
    sql_query = "SELECT * FROM "+project+"__"+parameter+" WHERE timeutc BETWEEN '"+start_date+" 00:00:00' AND '"+end_date+" 23:59:59'"
    with engine.connect() as connection:
        data_temp = connection.execute(text(sql_query))
        data = pd.DataFrame(data_temp.fetchall())
        if data.empty: return pd.DataFrame()
        data.columns = data_temp.keys()
        connection.commit()
    data = (
        data
        .query("mode == 'General'")
        .assign(Date=lambda data: pd.to_datetime(data["timelocal"], format="%Y-%m-%d %H:%M:%S"))
        .sort_values(by="Date")
    )
    connection.commit()
    return data

def update_data(project, parameter_01, parameter_02, parameter_03, parameter_04, date_range_selected, start_date, end_date):
    data_01 = pd.DataFrame()
    data_02 = pd.DataFrame()
    data_03 = pd.DataFrame()
    data_04 = pd.DataFrame()
    if project != None and parameter_01 != None and date_range_selected:
        data_01 = retrieve_data(project, parameter_01, start_date, end_date)
    if project != None and parameter_02 != None and date_range_selected:
        data_02 = retrieve_data(project, parameter_02, start_date, end_date)
    if project != None and parameter_03 != None and date_range_selected:
        data_03 = retrieve_data(project, parameter_03, start_date, end_date)
    if project != None and parameter_04 != None and date_range_selected:
        data_04 = retrieve_data(project, parameter_04, start_date, end_date)
    return data_01, data_02, data_03, data_04

def get_date_range(project, parameter):
    sql_query = "SELECT min(timelocal), max(timelocal) FROM "+project+"__"+parameter
    with engine.connect() as connection:
        data_temp = connection.execute(text(sql_query))
        data = data_temp.fetchone()
        connection.commit()
    connection.commit()
    return data._mapping["min"], data._mapping["max"]

app = Dash(__name__)

auth = dash_auth.BasicAuth(app, USER_PASSWORD_MAPPING)

app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.H1(children="Water Dashboard", className="header-title"), 
                html.H4(children="Institute for Water Quality and Resource Management", className="header-subtitle"),
            ],
            className="header",
        ),

    html.Div(
            children=[
            html.Div(
                    children=[
                        html.Div(children="Project", className="menu-title"),
                        dcc.Dropdown(
                            id="project-picker-1",
                            options=[
                                {"label": parameter, "value": parameter}
                                for parameter in list(parameter_tables.keys())
                            ],
                            className="dropdown",
                        ),
                    ]
                ),
            html.Div(
                children=[
                    html.Div(children="Date Range", className="menu-title "),
                    dls.Hash(
                        dcc.DatePickerRange(
                            id="date-range",
                            disabled=True,
                            initial_visible_month="2024-01-01"
                        ),
                    )
                ]
            ),
        ],
        className="menu",
    ),

    dcc.Store(id='memory-1'),

    html.Div(
            children=[
            html.Div(
                    children=[
                        html.Div(children="Parameter 1", className="menu-title"),
                        dcc.Dropdown(
                            id="parameter-picker-1",
                            className="dropdown",
                        ),
                        dcc.RadioItems([
                                {'label': 'Left Axis', 'value': 'left'},
                                {'label': 'Right Axis', 'value': 'right'}], 
                                value='left',
                                id="parameter-axis-1")
                    ],
                    className="parameter-div",
                ),
            html.Div(
                children=[
                    html.Div(children="Parameter 2", className="menu-title"),
                        dcc.Dropdown(
                            id="parameter-picker-2",
                            className="dropdown",
                            disabled=True
                        ),
                        dcc.RadioItems([
                                {'label': 'Left Axis', 'value': 'left', 'disabled':True},
                                {'label': 'Right Axis', 'value': 'right', 'disabled':True}], 
                                value='right',
                                id="parameter-axis-2"),
                ],
                className="parameter-div",
            ),
            html.Div(
                children=[
                    html.Div(children="Parameter 3", className="menu-title bottom-menu-title"),
                        dcc.Dropdown(
                            id="parameter-picker-3",
                            className="dropdown",
                            disabled=True
                        ),
                        dcc.RadioItems([
                                {'label': 'Left Axis', 'value': 'left', 'disabled':True},
                                {'label': 'Right Axis', 'value': 'right', 'disabled':True}], 
                                value='right',
                                id="parameter-axis-3"),
                ],
                className="parameter-div",
            ),
            html.Div(
                children=[
                    html.Div(children="Parameter 4", className="menu-title bottom-menu-title"),
                        dcc.Dropdown(
                            id="parameter-picker-4",
                            className="dropdown",
                            disabled=True
                        ),
                        dcc.RadioItems([
                                {'label': 'Left Axis', 'value': 'left', 'disabled':True},
                                {'label': 'Right Axis', 'value': 'right', 'disabled':True}], 
                                value='right',
                                id="parameter-axis-4"),
                ],
                className="parameter-div",
            ),
        ],
        className="menu",
    ),
    html.Div(
        dls.Hash(
            dcc.Graph(
                id="water-dashboard",
                config={"displayModeBar": False},
            className="card",
            ),
        )
    ),
    ],
    className="wrapper",    
)

def get_figure_data_entries(parameter, data, is_parameter_01, date_range_selected, axis):
    return {
                "x": None if data.empty or not date_range_selected else data["Date"],
                "y": None if data.empty or not date_range_selected else data["scal"],
                "name": parameter,
                "type": "lines",
                "yaxis": "y1" if axis == "left" else "y2",
                "hovertemplate": (
                    "%{y:.2f}<extra></extra>"
                ),
            }
    
def get_figure_layout(overlay):
    layout = {
                "legend":{"yanchor":"top","y":1.5,"xanchor":"right","x":1},
                "xaxis": {"fixedrange": True},
                "yaxis": {"fixedrange": True,},
                "colorway": ["#2481ff", "#17b897", "#d6c927", "#a30eaf"],
            }
    if overlay:
        layout["yaxis2"] = {"fixedrange": True, "overlaying":'y', "side":"right"}
    return layout 

def remove_none_values(list):
    return [i for i in list if i is not None]

def get_figure(data, overlay):
    data = remove_none_values(data)
    return {
            "data": data,
            "layout": get_figure_layout(overlay),
        }

def get_parameter_options(parameters):
    return [{"label": parameter, "value": parameter} for parameter in parameters]

def get_remaining_parameters(parameter_list, parameters_to_remove, parameter_not_to_remove):
    remaining_parameters = parameter_list.copy()
    for par in remove_none_values(parameters_to_remove):
        if parameter_not_to_remove is None or parameter_not_to_remove != par: remaining_parameters.remove(par)
    return remaining_parameters

def get_axis_options(is_disabled):
    return [{'label': 'Left Axis', 'value': 'left', 'disabled':is_disabled},{'label': 'Right Axis', 'value': 'right', 'disabled':is_disabled}]

def pre_retrieve_data(project, parameter, start_date, end_date):
    date_range_selected = not start_date == None and not end_date == None
    if project is not None and parameter is not None and date_range_selected:
        return retrieve_data(project, parameter, start_date, end_date), True
    return pd.DataFrame(), False

def update_global_data(index, updated_data):
    global global_data
    global_data[index] = updated_data
    return global_data

def update_global_dates(is_start, date):
    global global_date
    if is_start: 
        global_date[list(global_date.keys())[0]] = date 
    else:
        global_date[list(global_date.keys())[1]] = date 
    return global_date

def remove_from_global_data(indices):
    global global_data
    for i in indices:
        global_data[i] = None
    return global_data

def set_data(project, parameter, start_date, end_date, axis, index):
    counter_index = int(index.split("-")[1])-1
    if len(parameter) > 1: counter_index = 0
    for p in parameter:
        raw_data, date_range_selected = pre_retrieve_data(project, p, start_date, end_date)
        data = get_figure_data_entries(p, raw_data, True, date_range_selected, axis)
        if data["x"] is not None:
            update_global_data(list(global_data.keys())[counter_index], data)
            counter_index = counter_index + 1

@app.callback(
    Output("water-dashboard", "figure"),
    Input("project-picker-1", "value"),
    Input("parameter-picker-1", "value"),
    Input("parameter-picker-2", "value"),
    Input("parameter-picker-3", "value"),
    Input("parameter-picker-4", "value"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    Input("parameter-axis-1", "value"),
    Input("parameter-axis-2", "value"),
    Input("parameter-axis-3", "value"),
    Input("parameter-axis-4", "value")
)
def get_parameter_1_data(project, parameter_01, parameter_02, parameter_03, parameter_04, start_date, end_date, axis_01, axis_02, axis_03, axis_04):
    input_element = ctx.triggered_id
    date_range_selected = not start_date == None and not end_date == None
    axis = axis_01
    index = list(global_data.keys())[0]
    parameter = [parameter_01]
    more_than_one_parameter = False
    if input_element == "parameter-picker-2" or input_element == "parameter-axis-2":
        index = list(global_data.keys())[1]
        parameter = [parameter_02]
        axis = axis_02
        more_than_one_parameter = True
    elif input_element == "parameter-picker-3" or input_element == "parameter-axis-3":
        index = list(global_data.keys())[2]
        parameter = [parameter_03]
        axis = axis_03
        more_than_one_parameter = True
    elif input_element == "parameter-picker-4" or input_element == "parameter-axis-4":
        index = list(global_data.keys())[3]
        parameter = [parameter_04]
        axis = axis_04
        more_than_one_parameter = True
    elif input_element == "date-range" and date_range_selected:
        parameter = remove_none_values([parameter_01, parameter_02, parameter_03, parameter_04])
    set_data(project, parameter, start_date, end_date, axis, index)
    return get_figure([global_data[list(global_data.keys())[0]], global_data[list(global_data.keys())[1]], global_data[list(global_data.keys())[2]], global_data[list(global_data.keys())[3]]], more_than_one_parameter)

@app.callback(
Output("parameter-picker-1", "options"),
Output("parameter-picker-2", "options"),
Output("parameter-picker-2", "disabled"),
Output("parameter-picker-2", "value"),
Output("parameter-axis-2", "options"),
Output("parameter-picker-3", "options"),
Output("parameter-picker-3", "disabled"),
Output("parameter-picker-3", "value"),
Output("parameter-axis-3", "options"),
Output("parameter-picker-4", "options"),
Output("parameter-picker-4", "disabled"),
Output("parameter-picker-4", "value"),
Output("parameter-axis-4", "options"),
Input("project-picker-1", "value"),
Input("parameter-picker-1", "value"),
Input("parameter-picker-2", "value"),
Input("parameter-picker-3", "value"),
Input("parameter-picker-4", "value")
)
def update_parameter_other_parameters(project, parameter_01, parameter_02, parameter_03, parameter_04):
    if parameter_01 == None or project == None:
        remove_from_global_data(["parameter-1","parameter-2","parameter-3","parameter-4"])
        if project == None:
            return get_parameter_options([]), get_parameter_options([]), True, None, get_axis_options(True), get_parameter_options([]), True, None, get_axis_options(True), get_parameter_options([]), True, None, get_axis_options(True)
        return get_parameter_options(parameter_tables[project]), get_parameter_options([]), True, None, get_axis_options(True), get_parameter_options([]), True, None, get_axis_options(True), get_parameter_options([]), True, None, get_axis_options(True)
    all_parameters = parameter_tables[project].copy()
    active_parameters = remove_none_values([parameter_01, parameter_02, parameter_03, parameter_04])
    parameter_01_list = get_remaining_parameters(all_parameters,active_parameters, parameter_01)
    parameter_02_list = get_remaining_parameters(all_parameters,active_parameters, parameter_02)
    parameter_03_list = get_remaining_parameters(all_parameters,active_parameters, parameter_03)
    parameter_04_list = get_remaining_parameters(all_parameters,active_parameters, parameter_04)
    if parameter_02 == None:
        remove_from_global_data(["parameter-2","parameter-3","parameter-4"])
        return get_parameter_options(parameter_01_list), get_parameter_options(parameter_02_list), False, parameter_02, get_axis_options(False), get_parameter_options([]), True, None, get_axis_options(True), get_parameter_options([]), True, None, get_axis_options(True)
    if parameter_03 == None:
        remove_from_global_data(["parameter-3","parameter-4"])
        return get_parameter_options(parameter_01_list), get_parameter_options(parameter_02_list), False, parameter_02, get_axis_options(False), get_parameter_options(parameter_03_list), False, parameter_03, get_axis_options(False), get_parameter_options([]), True, None, get_axis_options(True)
    if parameter_04 == None:
        remove_from_global_data(["parameter-4"])
        return get_parameter_options(parameter_01_list), get_parameter_options(parameter_02_list), False, parameter_02, get_axis_options(False), get_parameter_options(parameter_03_list), False, parameter_03, get_axis_options(False), get_parameter_options(parameter_04_list), False, None, get_axis_options(False)
    else:
        return get_parameter_options(parameter_01_list), get_parameter_options(parameter_02_list), False, parameter_02, get_axis_options(False), get_parameter_options(parameter_03_list), False, parameter_03, get_axis_options(False), get_parameter_options(parameter_04_list), False, parameter_04, get_axis_options(False)

@app.callback(
Output("date-range", "disabled"),
Output("date-range", "min_date_allowed"),
Output("date-range", "max_date_allowed"),
Output("date-range", "start_date"),
Output("date-range", "end_date"),
Output("date-range", "initial_visible_month"),
Input("project-picker-1", "value"),
Input("parameter-picker-1", "value"),
Input("date-range", "start_date"),
Input("date-range", "end_date"),
Input("date-range", "min_date_allowed"),
Input("date-range", "max_date_allowed"),
)
def fill_date_picker(project, parameter, start_date, end_date, min_date, max_date):
    dates_unchanged = start_date == global_date[list(global_date.keys())[0]] and start_date == global_date[list(global_date.keys())[1]]
    date_is_picked = start_date is not None or end_date is not None
    if parameter == None or project == None:
        return True, None, None, None, None, "2024-01-01"
    if (dates_unchanged and date_is_picked) or date_is_picked:
        return False, min_date, max_date, start_date, end_date, end_date
    min_date, max_date = get_date_range(project, parameter)
    return False, min_date, max_date, None, None, max_date

if __name__ == "__main__":
    app.run_server(debug=True)