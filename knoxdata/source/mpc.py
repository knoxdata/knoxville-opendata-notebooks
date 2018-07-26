import json
from collections import defaultdict
import sqlite3

import requests
import pandas as pd
import numpy as np


def get_and_write_mpc_data():
    # determined from website usage for bounds
    knoxville_bounds = [
        [2509746.9416408655, 2659731.9416408655], # lat
        [567692.794594815, 639302.794594815], # long
    ]
    query_area = 1000.0

    latitudes = np.linspace(knoxville_bounds[0][0], knoxville_bounds[0][1], math.ceil((knoxville_bounds[0][1] - knoxville_bounds[0][0]) / query_area))
    longitudes = np.linspace(knoxville_bounds[1][0], knoxville_bounds[1][1], math.ceil((knoxville_bounds[1][1] - knoxville_bounds[1][0]) / query_area))

    print(len(latitudes), len(longitudes), 'total queries', len(latitudes) * len(longitudes))
    # Caching will be very usefull (but request_cache doesn't work.......)
    conn = sqlite3.connect('../data/mpc/mpc.sqlite3')

    geo_data = set()
    session = requests.session()
    session.get('https://www.kgis.org/maps/MPCCases.html') # get cookies

    for latitude in latitudes:
        for longitude in longitudes:
            if (latitude, longitude) in geo_data:
                continue
            query_mpc_inventory(latitude, longitude, query_area, session, conn)
            geo_data.add((latitude, longitude))


def insert_df_into_table(conn, table_name, df):
    df1 = df.reset_index()
    column_names = ', '.join(['"%s"' % name for name in df1.columns])
    fields = ','.join(['?'] * len(df1.columns))
    replace_str = 'REPLACE INTO "%s" (%s) VALUES (%s)' % (table_name, column_names, fields)

    with conn as c:
        c.executemany(replace_str, df1.values.tolist())


def create_table_from_df(conn, table_name, df):
    dtype_conversion = {
        np.float64: 'REAL',
        np.object_: 'TEXT',
        np.int64: 'INTEGER'
    }

    table_schema = []
    table_schema.append('"%s" %s PRIMARY KEY NOT NULL' % (df.index.name, dtype_conversion[df.index.dtype.type]))
    for name, t in zip(df.columns, df.dtypes):
        table_schema.append('"%s" %s' % (name, dtype_conversion[t.type]))

    with conn as c:
        c.execute('CREATE TABLE IF NOT EXISTS "%s" (%s)' % (table_name, ', '.join(table_schema)))


def query_mpc_inventory(latitude, longitude, area_square, session, conn):
    query = {
        'returnGeometry': True,
        'spatialRel': 'esriSpatialRelIntersects',
        'geometry': json.dumps({
            "rings":[[
                [latitude, longitude+area_square],
                [latitude+area_square, longitude+area_square],
                [latitude+area_square, longitude],
                [latitude, longitude],
                [latitude, longitude+area_square]
            ]],
            "spatialReference":{"wkid":2915}
        }),
        'geometryType': 'esriGeometryPolygon',
        'inSR': 2915,
        'outFields': '*',
        'outSR': 2915
    }

    headers = {
        'Referer': 'https://www.kgis.org/maps/MPCCases.html'
    }

    data_sources = {
        'https://www.kgis.org/arcgis/rest/services/Maps/TreeInventory/MapServer/%d/query?f': (0, 1, 2, 3, 4),
        'https://www.kgis.org/arcgis/rest/services/Maps/GlobalSearch/MapServer/%d/query?f': (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25),
        'https://www.kgis.org/arcgis/rest/services/Maps/MPCCases/MapServer/%d/query?f': (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28),
        'https://www.kgis.org/arcgis/rest/services/Maps/GrowthPlan/MapServer/%d/query?f': (0, 1, 2),
        'https://www.kgis.org/arcgis/rest/services/Maps/MPCCasesScenicLayers/MapServer/%d/query?f': (0, 1, 2),
        'https://www.kgis.org/arcgis/rest/services/Maps/SchoolPRZ/MapServer/%d/query?f': (0, 1, 2, 3),
        'https://www.kgis.org/arcgis/rest/services/Maps/TelecommTowers/MapServer/%d/query?f': (0,),
    }

    for partial_url, indicies in data_sources.items():
        for i in indicies:
            url = partial_url % (i)
            try:
                response = session.get('http://www.kgis.org/proxy/proxy.ashx', params={url: 'json', **query}, headers=headers)
                response_json = response.json()
                if response_json.get('error', {}).get('code') == 400:
                    raise Exception()
                url_data = []
                for row in response_json['features']:
                    url_data.append({'geometry': json.dumps(row['geometry']), **row['attributes']})
                if url_data:
                    df = pd.DataFrame(url_data).set_index('OBJECTID')
                    create_table_from_df(conn, url, df)
                    insert_df_into_table(conn, url, df)
            except Exception:
                print(response.content)
                print('failed for url: ', url)
