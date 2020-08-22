import collections
import multiprocessing
import os
import time
import json
import requests
import pandas as pd
import geopandas as gpd
import os
from pyproj import Proj, transform
from shapely.geometry import Point, LineString, MultiLineString
import matplotlib.pyplot as plt

import numpy as np

import geopy.distance
from shapely.geometry import Polygon, MultiPolygon


def organiza_geom(df):
    
    size = len(df["type"].unique())

    if size == 1:
        if df["type"].unique() == 'Point':
            
            df["coordinates"] = [Point(df["coordinates"][i]) for i in range( len(df) ) ]
            
    else:
        for i in range(len(df['type'])):
            
            cond = df["type"].values[i]
            
            foo = []
            if cond == 'LineString':
               
                df["coordinates"] = foo.append(LineString(df["coordinates"][i]) ) 
                
            elif cond == 'MultiLineString':
               
                df["coordinates"] = foo.append(MultiLineString(df["coordinates"][i]) )

    del df["type"]
    
            
    return df
            
def datos_organizados(json_list, column_list):

    """
    Esta función se encarga de recibir la lista con la información
    descargada y la información de las columnas que se dejaran,
    la organiza para dejarla como DataFrame
    """
    
    df_final     = [json_list["features"][x]["properties"] for x in range( len( json_list["features"] ) )]
    df_final_geo = [json_list["features"][x]["geometry"] for x in range( len( json_list["features"] ) )]

    df_final = gpd.GeoDataFrame(df_final)
    df_final_geo = gpd.GeoDataFrame(df_final_geo)
    df_final = df_final[column_list]
    df_final_geo["OBJECTID"] = df_final_geo.index
    df_final = pd.merge(df_final, df_final_geo, on="OBJECTID")
    
    if len(df_final["type"].unique()) == 1 and df_final["type"].unique() == 'Point':

        #df_final["coordinates"] = [Point(df_final["coordinates"][i]) for i in range( len(df_final) ) ]
        df_long = [df_final.coordinates[i][0] for i in range(len(df_final)) ] 
        df_lat =  [df_final.coordinates[i][1] for i in range(len(df_final)) ] 
        df_final = gpd.GeoDataFrame(df_final, geometry=gpd.points_from_xy(df_long, df_lat))
        del df_final["type"]
        del df_final["coordinates"]

    return df_final


file1 = "nomen_data.txt"
file2 = "poi_data.txt"
file3 = "lim_cat_data.txt"
file4 = "lim_barrio_data.txt"
file5 = "codigo_postal_data.txt"
file6 = "malla_vial_data.txt"

file = [file1, file2, file3 ,file4, file5, file6]

if file1 and file2 and file3 and file4 and file5 and file6:
    print("Los archivos existen. Cargando...")
    with open(file1) as json_file:
        nomen_data = json.load(json_file)

    with open(file2) as json_file:
        poi_data = json.load(json_file)

    with open(file3) as json_file:
        lim_cat_data = json.load(json_file)

    with open(file4) as json_file:
        lim_barrio_data = json.load(json_file)

    with open(file5) as json_file:
        codigo_postal_data = json.load(json_file)

    with open(file6) as json_file:
        malla_vial_data = json.load(json_file)

else:
    print("Los archivos no existen. Descargando y cargando...")

    url_nomen = 'https://opendata.arcgis.com/datasets/a7a752b72ffd45bda330b975aac26ce5_3.geojson'
    url_poi = 'https://opendata.arcgis.com/datasets/2cfddcc5e4bf415abd49560bdc36a9f7_2.geojson'
    url_lim_cat = 'https://opendata.arcgis.com/datasets/283d1d14584641c9971edbd2f695e502_6.geojson'
    url_lim_barrio = 'https://opendata.arcgis.com/datasets/1a6dbf15865b4357aa559699ea90c5b9_7.geojson'
    url_postal = 'https://opendata.arcgis.com/datasets/7e9a431be3674009ad57208ca0343a1d_11.geojson'
    url_malla_vial = 'https://opendata.arcgis.com/datasets/da89cc206c7e484d9f7ba35d81ca9742_0.geojson'

    nomen_data = requests.get(url_nomen).json()
    poi_data = requests.get(url_poi).json()
    lim_cat_data = requests.get(url_lim_cat).json()
    lim_barrio_data = requests.get(url_lim_barrio).json()
    codigo_postal_data = requests.get(url_postal).json()
    malla_vial_data = requests.get(url_malla_vial).json()

    data_names = ["nomen_data", "poi_data", "lim_cat_data", 
                  "lim_barrio_data","codigo_postal_data", 
                  "malla_vial_data"]

    data = [nomen_data, poi_data, lim_cat_data, 
                  lim_barrio_data,codigo_postal_data, 
                  malla_vial_data]

    for i in range(len(data)):
        with open(data_names[i]+".txt","w") as outfile:
            json.dump(data[i], outfile)


column_name_nomen = ["OBJECTID", "TIPO_VIA", "ORIENTACION_VIA", "ORIENTACION_CRUCE",
                     "VIA", "PLACA","TIPO_CRUCE","X_MAGNAMED", "Y_MAGNAMED",
                     "DIRECCIONENCASILLADA", "NOMBRE_BARRIO", "CODIGO_COMUNA", "NOMBRE_COMUNA"]

column_name_poi = ["OBJECTID", "NOMBRE", "CODIGO_BARRIO", "NOMBRE_BARRIO", "CODIGO_COMUNA", "NOMBRE_COMUNA"]

column_name_lim_cat = ["OBJECTID", "COMUNA", "SECTOR", "NOMBRE"]

column_name_lim_barrio = ["OBJECTID", "COMUNA", "BARRIO", "CODIGO", "NOMBRE_BARRIO", "NOMBRE_COMUNA"]

column_name_postal = ["OBJECTID", "CODIGO_POSTAL", "ZONAPOSTALID"]

column_name_malla_vial = ["OBJECTID", "TIPO_VIA", "LABEL", "NOMBRE_COMUN", "LONGITUD"]

#######################################

print("Cargando en variables")

nomenclatura  = datos_organizados(nomen_data, column_name_nomen)
poi           = datos_organizados(poi_data, column_name_poi)
limite_catast = datos_organizados(lim_cat_data, column_name_lim_cat)
limite_barrio = datos_organizados(lim_barrio_data, column_name_lim_barrio)
codigo_postal = datos_organizados(codigo_postal_data, column_name_postal)
malla_vial    = datos_organizados(malla_vial_data, column_name_malla_vial)

################## PoI de Medellín descargados de la pagina https://extract.bbbike.org/
poi_pag_path = "shape/points.shp"
gpd_points = gpd.read_file(poi_pag_path)


def poi_en_barrio(limite_barrio, gpd_points):
    """
    Esta función se encarga de mirar si los puntos
    de gpd_points se encuentran dentro de un barrio 
    en específico y lo organiza para que quede en 
    el formato que tiene poi 
    """
    dictionary = []
    contador = 14434
    long = len(limite_barrio)
    for i in range(len(limite_barrio.coordinates)):
        barrio = limite_barrio.coordinates.loc[i][0]
        dic = {}
        for j in range(len(gpd_points.geometry)):
            point_poi = gpd_points.geometry.loc[j]


            if limite_barrio["type"].loc[i] == 'Polygon':
                punto_en_barrio = Polygon(barrio)
            else:
                punto_en_barrio = Polygon(barrio[0])
            if punto_en_barrio.contains(point_poi):
                I = i
                dic = {"OBJECTID": contador ,
                       "NOMBRE": gpd_points["type"].loc[j],
                       "CODIGO_BARRIO": limite_barrio["CODIGO"].loc[i],
                       "NOMBRE_BARRIO": limite_barrio["NOMBRE_BARRIO"].loc[i],
                       "CODIGO_COMUNA": limite_barrio["COMUNA"].loc[i] ,
                       "NOMBRE_COMUNA": limite_barrio["NOMBRE_COMUNA"].loc[i] ,
                       "geometry": point_poi
                }
                dictionary.append(dic)
                contador += 1
        print("Voy en {} de {}".format( I,long ) )
    np.save("diccionario.dat",dictionary )
    
    return dictionary

print("Empieza paralelización...")


start = time.time()

result = poi_en_barrio(limite_barrio, gpd_points) 
df = pd.DataFrame(result)
df.to_pickle("poi_en_barrio.pkl")

#grafica = pool.apply(histograma, (result))
end = time.time()
print("\n Time to complete:{}s\n".format(end - start))

"""if __name__ == '__main__':
    # start 4 worker processes
    with multiprocessing.Pool(processes = 4) as pool:
        
        start = time.time()

        result = pool.apply(poi_en_barrio, (limite_barrio, gpd_points) )
        df = pd.DataFrame(result)
        df.to_pickle("poi_en_barrio.pkl")

        #grafica = pool.apply(histograma, (result))
        end = time.time()

        print("\n Time to complete:{}s\n".format(end - start))"""

    
