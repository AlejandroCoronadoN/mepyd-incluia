# pandas stack
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Point
import geopandas as gpd
import os
from projectetl.utils.process import name_normalizer

# get timestamp in utc format
from datetime import datetime

def process_admindiv(gdf_municipalities, gdf_neighborhood, df_provinces_codes, gdf_caipi):
    #Simplyfy 
    do_munis_simplified_gdf = gdf_municipalities.copy()
    do_munis_simplified_gdf = do_munis_simplified_gdf.to_crs(crs="EPSG:4326")
    tolerance_macro = 9e-5 # 3e-3
    do_munis_simplified_gdf.loc[:, 'geometry'] = \
        do_munis_simplified_gdf.geometry.simplify(tolerance_macro)
    do_munis_simplified_gdf.plot(figsize=(20, 12))


    #Municipality Parse
    df_provinces_codes.loc[:,'Provincia'] = df_provinces_codes.loc[:,'Provincia'].str.strip()
    do_munis_simplified_gdf.loc[:, 'Provincia'] = do_munis_simplified_gdf['ADM2_ES'].str.replace('Provincia', '').str.strip(' ').str.strip('\n').str.upper()

    merged_gdf = do_munis_simplified_gdf.merge(df_provinces_codes, how='outer', indicator=True, on='Provincia')
    merged_gdf.drop('_merge', axis=1, inplace=True)

    # build fields for merge and map display
    merged_gdf.loc[:, 'municipality_key'] = merged_gdf['Código de   provincia'].astype(str).str.zfill(2).str.cat(merged_gdf.ADM3_PCODE.str.slice(-2))
    merged_gdf.loc[:, 'Municipio'] = merged_gdf['ADM3_ES'].str.replace('Municipio', '').str.strip(' ').str.strip('\n')
    merged_gdf.loc[:, 'Provincia'] = merged_gdf.loc[:, 'Provincia'].str.title()

    #Number of  CAIPIS
    # municipality name match
    normalized_muni_col = 'norm_municipio'
    caipi_stats_df = gdf_caipi.assign(**{
        normalized_muni_col: lambda df: name_normalizer(df.Municipio).str.replace(
            'bisono (navarrete)', 'bisono', regex=False)}
                                    # ).dropna(subset=['Longitud']
                                            ).groupby(
        normalized_muni_col, as_index=False)[['NumCentro']].count()
    caipi_stats_df.shape

    gdf_municipality_processed = merged_gdf.assign(**{
        normalized_muni_col: lambda df: name_normalizer(df.Municipio)}).merge(
        caipi_stats_df, how='outer', indicator=True).rename(
        columns={'NumCentro': 'caipi_sum'})
    del  gdf_municipality_processed['_merge']

    #Neighborhood
    gdf_neighborhood = gdf_neighborhood.to_crs(crs="EPSG:4326")
    tolerance_macro = 9e-5 # 3e-3
    gdf_neighborhood.loc[:, 'geometry'] = gdf_neighborhood.geometry.simplify(tolerance_macro)
    gdf_neighborhood.plot(figsize=(20, 12))

    gdf_neighborhood['municipality_key'] = gdf_neighborhood['PROV'] + gdf_neighborhood['MUN'] 
    gdf_neighborhood['neighborhood_key'] =  gdf_neighborhood['PROV'] + gdf_neighborhood['MUN'] + gdf_neighborhood['DM'] + gdf_neighborhood['SECC'] + gdf_neighborhood['BP']

    #Number of CAIPIS
    df_neighbor_caipi = gpd.sjoin(gdf_caipi, gdf_neighborhood, op='within')
    df_neighbor_caipi= gpd.sjoin(gdf_caipi, gdf_neighborhood, op='within')
    #Not within CAPIS 
    df_neighbor_caipi_left = gpd.sjoin(gdf_caipi, gdf_neighborhood, op='within', how='left')
    df_neighbor_caipi_notfound = df_neighbor_caipi_left[df_neighbor_caipi_left.PROV.isna()]

    # df_neighbor_caipi_stats: how many CAPI's we have in each neighborhood
    df_neighbor_caipi_stats = df_neighbor_caipi\
        .groupby('neighborhood_key')[['NumCentro']].count()\
        .reset_index()\
        .rename(columns = {'NumCentro': 'caipi_sum_neighbor'})

    gdf_neighborhood_stats = gdf_neighborhood.merge(df_neighbor_caipi_stats, on ='neighborhood_key', how= 'left')
    gdf_neighborhood_stats.columns

    #We can't have multiple geomtry variables in a single geo file
    gdf_municipality_processed_merge = gdf_municipality_processed.copy()
    del gdf_municipality_processed_merge['geometry']

    gdf_neighborhood_processed  = gdf_municipality_processed_merge.merge(gdf_neighborhood_stats, on = 'municipality_key', how='outer', indicator =True)
    gdf_neighborhood_processed.drop(['_merge'], axis=1, inplace=True)

    #EXPORT
    gdf_neighborhood_processed['TOPONIMIA'] = gdf_neighborhood_processed['TOPONIMIA'].str.title()
    #gdf_neighborhood_processed.to_file(f'data/preprocess/preprocess_neighborhood.geojson', driver="GeoJSON")

    cols_municipalities_gpd = [
                'Provincia', 'Municipio', 'municipality_key', 'caipi_sum',
                'geometry'
        ]
    cols_neighborhood_gpd = [
                'Provincia', 'TOPONIMIA', 'neighborhood_key', 'caipi_sum_neighbor',
                'geometry','municipality_key', 'Municipio'
        ]
    gdf_neighborhood_processed = gdf_neighborhood_processed[cols_neighborhood_gpd]
    gdf_municipality_processed = gdf_municipality_processed[cols_municipalities_gpd]
    
    return gdf_neighborhood_processed, gdf_municipality_processed
