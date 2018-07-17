import json
from collections import defaultdict

import requests
import pandas as pd


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

    geo_data = {}
    session = requests.session()
    session.get('https://www.kgis.org/maps/MPCCases.html') # get cookies

    for latitude in latitudes:
        for longitude in longitudes:
            if (latitude, longitude) in geo_data:
                continue
            geo_data[(latitude, longitude)] = query_mpc_inventory(latitude, longitude, query_area, session)

    geo_pd_data = defaultdict(list)
    for coord, data in geo_data.items():
        for url, values in data.items():
            if values is not None:
                geo_pd_data[url].append(values)

    df_data = {}
    for url in geo_pd_data:
        df = pd.concat(geo_pd_data[url]).drop_duplicates()
        df_data[url] = df
        striped_url = url.remove('/')
        df.to_csv('../data/mpc/%s.csv' % striped_url)

    return df_data


def query_mpc_inventory(latitude, longitude, area_square, session):
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

    data = {}
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
                    data[url] = pd.DataFrame(url_data).set_index('OBJECTID')
                else:
                    data[url] = None
            except Exception:
                print('failed for url: ', url)
                data[url] = None
    return data
