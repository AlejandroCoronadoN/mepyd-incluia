import logging, sys
import pandas as pd 
import numpy as np 
import geopandas as gpd
from scipy.stats import hmean
from sklearn.preprocessing import MinMaxScaler
from tqdm import tqdm
from criteriaetl.utils.common_func import (get_weighted_complete_randomization_series_on_subset, 
    proportional_cut, weighted_qcut, get_partition_bool_columns_dict)
from criteriaetl.transformers.columns_base import (NameTransformer, 
    ReplaceTransformer, SelectTransformer, AssignTransformer)
from criteriaetl.transformers.rows_base import (
    AggregateTransformer, DropTransformer)
from criteriaetl.transformers.fusion_base import MergeTransformer
from utils.u0_utils import areaoverlay_capacity_assignment, dissolve_dataframe_areas, read_csv_idobject
import warnings
from projectetl.utils.config import data_dir
import logging
from loadData.loa1_loadInfo import load_data

#Ignore warnings in this file to avoid unncesary print Wanings 
warnings.filterwarnings('ignore')

def get_child_stats(merged_superate_gdf, key_col='neighborhood_key'):
    child_accessibility_neigh_src = merged_superate_gdf.loc[lambda df: df['poblacion_objetivo'].eq(
                'primera infancia')].reset_index().assign(**{
                'type': lambda df: df.tipo.str.split(' - ', expand=True).fillna(
                        axis=1, method='ffill').iloc[:, -1]
        }).groupby([key_col, 'type', 'index'], as_index=True)[[
                'superate_child_0to4_receiver_sum']].nth(0).reset_index()

    child_with_access_neigh_df = pd.pivot_table(
            child_accessibility_neigh_src, 
            values='superate_child_0to4_receiver_sum', index=key_col, 
            columns='type', aggfunc='sum')
    child_with_access_neigh_df = child_with_access_neigh_df.rename(columns={
            'CAIPI': 'accesible_child_0to4_receiver_caipi',
            'CCPP CON EPES': 'accesible_child_0to4_receiver_ccpp_epes'
    })
    #child_with_access_neigh_df = child_with_access_neigh_df.reset_index()
    return child_with_access_neigh_df

def get_elder_severe_stats(merged_superate_gdf, key_col='neighborhood_key'):
    ''' Esta función procesa los adultos mayores dependientes y dependientes severos utilizando
    
    '''
    elder_accessibility_neigh_src = merged_superate_gdf.loc[lambda df: df['poblacion_objetivo'].eq(
                    'adulto mayor dependiente')].reset_index().assign(**{
                    'type': lambda df: df.tipo.str.split(' - ', expand=True).fillna(
                            axis=1, method='ffill').iloc[:, -1]
            }).groupby([key_col, 'type', 'index'], as_index=True)[[
                    'superate_older_dependant_sum', 'superate_older_severe_dependant_sum']
    ].nth(0).reset_index()
    elder_with_access_neigh_df = pd.pivot_table(
            elder_accessibility_neigh_src, 
            values='superate_older_dependant_sum', index=key_col, 
            columns='type', aggfunc='sum').fillna(0)
    elder_with_access_neigh_df = elder_with_access_neigh_df.rename(columns={
            'ASFL DIURNAS': 'accesible_elder_asfl_diurna', 
            'ASFL PERMANENTES': 'accesible_elder_asfl_permanente', 
            'ESTANCIAS CONAPE': 'accesible_elder_estancias_conape', 
            'Nuevos C. de Dia CONAPE': 'accesible_elder_nuevos_conape'
    })
    severe_with_access_neigh_df = pd.pivot_table(
            elder_accessibility_neigh_src, 
            values='superate_older_severe_dependant_sum', index=key_col, 
            columns='type', aggfunc='sum').fillna(0)
    severe_with_access_neigh_df = severe_with_access_neigh_df.rename(columns={
            'ASFL DIURNAS': 'accesible_severe_asfl_diurna', 
            'ASFL PERMANENTES': 'accesible_severe_asfl_permanente', 
            'ESTANCIAS CONAPE': 'accesible_severe_estancias_conape', 
            'Nuevos C. de Dia CONAPE': 'accesible_severe_nuevos_conape'
    })
    #elder_with_access_neigh_df = elder_with_access_neigh_df.reset_index()
    #severe_with_access_neigh_df = severe_with_access_neigh_df.reset_index()
    return elder_with_access_neigh_df, severe_with_access_neigh_df

    
def get_centers_stats(gdf_centers, gdf_admindiv, centers_areas_car_walk, key_col = 'neighborhood_key'):
    gdf_admindiv = gdf_admindiv.to_crs("EPSG:4326")
    centers_areas_car_walk = centers_areas_car_walk.to_crs('EPSG:4326')
    
    df_cap_assigned = pd.DataFrame()
    centers_capacity = gdf_centers.assign(**{
        'tipo_rename': lambda df: df.tipo.str.split(' - ', expand=True).fillna(
            axis=1, method='ffill').iloc[:, -1]})

    for center_type in tqdm(centers_capacity.tipo_rename.unique(), 'Stata/get_centers_stats'):
        print('\n\n', center_type)
        centers_capacity['id'] = pd.to_numeric(centers_capacity.id)
        centers_areas_car_walk['id'] = pd.to_numeric(centers_areas_car_walk['id'])
        
        df_center_cap = centers_capacity[centers_capacity.tipo_rename == center_type][['tipo_rename',key_col, 'capacidad', 'id']]
        #print("df_center_cap: ", df_center_cap.shape)
        if df_center_cap.capacidad.isna().all():
            #Casos especiales donde hay numero de oficinas pero no capacidad
            df_center_cap['capacidad'] = 1
        df_center_geom = df_center_cap.merge(centers_areas_car_walk, on ='id', how ='left', indicator=True)
        if np.sum(df_center_geom._merge =='left_only'):
            print('Center not found... starting gradinet descending')
            #We have some data that is missing it's influence area
            gdf_noinfluencearea = df_center_geom[df_center_geom._merge =='left_only'].copy()
            del gdf_noinfluencearea['geometry']
            #We need the area of the points with matchined influence area. 
            gdf_influencearea = gpd.GeoDataFrame(df_center_geom[df_center_geom._merge =='both'].copy())
            existing_meanarea = gdf_influencearea.area.mean()

            #We want to use the latitude and longitude of the center in order to create an influence area
            gdf_noinfluencearea_points = gdf_noinfluencearea.merge(centers_capacity[['id', 'geometry']])
            gdf_noinfluencearea_points = gpd.GeoDataFrame(gdf_noinfluencearea_points)
            diff =100
            alpha = .1
            while np.abs(diff) >.001:
                buffer_meanarea = gdf_noinfluencearea_points['geometry'].buffer(alpha).area.mean()
                diff = existing_meanarea - buffer_meanarea
                alpha = alpha + (diff *.01)
                
            gdf_noinfluencearea_points['geometry'] = gdf_noinfluencearea_points['geometry'].buffer(alpha)
            df_center_geom = gdf_influencearea.append(gdf_noinfluencearea_points)
            del df_center_geom['_merge']
            
        df_center_geom = gpd.GeoDataFrame(df_center_geom, crs = "EPSG:4326")
        #print('df_center_geom: ', df_center_geom.shape)

        df_overlay = areaoverlay_capacity_assignment(gdf_admindiv , df_center_geom, key_col, 'id', 'capacidad' )
        #print('df_overlay: ', df_overlay.shape)
        df_overlay = df_overlay.rename(columns={'capacidad':center_type})

        if len(df_cap_assigned) ==0:
            df_cap_assigned = df_overlay
        else:
            df_cap_assigned = df_cap_assigned.merge(df_overlay, how ='outer')
        
    df_cap_assigned = df_cap_assigned.assign(**{
        'TOTAL CONAPE': lambda df: df[[
        'ASFL DIURNAS', 'ASFL PERMANENTES', 'ESTANCIAS CONAPE', 
            'Nuevos C. de Dia CONAPE'
        ]].sum(1)})    

    rename_dict = {'CAIPI':'capacidad total (CAIPI)', 
        'COS INFOTEP':'# COS INFOTEP', 
        'ASFL DIURNAS': 'capacidad total (ASFL DIURNAS)',
        'ESTANCIAS CONAPE': 'capacidad total (ESTANCIAS CONAPE)', 
        'CCPP SIN EPES': '# CCPP SIN EPES' , 
        'Oficina de la Mujer': '# Oficina de la Mujer',
        'Direccion Regional PROSOLI': '# Direccion Regional PROSOLI', 
        'CTC CON EPES': 'capacidad total (CTC CON EPES)', 
        'CCPP CON EPES': 'capacidad total (CCPP CON EPES)',
        'Nuevos C. de Dia CONAPE':'capacidad total (Nuevos C. de Dia CONAPE)' , 
        'Direccion Regional INFOTEP': '# Direccion Regional INFOTEP' ,
        'ASFL PERMANENTES': 'capacidad total (ASFL PERMANENTES)', 
        'CAID':'# CAID' ,
        'TOTAL CONAPE':'capacidad total (TOTAL CONAPE)'}

    df_cap_assigned = df_cap_assigned.rename(columns =rename_dict)
    df_cap_assigned = df_cap_assigned.set_index(key_col)
    return df_cap_assigned

def join_stats(df_cap_assigned, child_with_access_df, elder_with_access_df, severe_with_access_df):
    '''In the previous functions we have created multiple variables that report statistics related to each center.
    Each center has it's own targeted group. 
    1. Children between 0 and 4 years
    2.  disabled people
    3.  Elder dependant 
    In this fucntion all these variables are merge into a single DataFrame
    '''
    #Merge operation with how = outer will select all observations 
    # Join will select only those that have values in all the dataFrames taht are being joined. 
    centers_per_neigh_stats_df = df_cap_assigned.merge(child_with_access_df, left_index = True, right_index =True, how = 'outer')
    centers_per_neigh_stats_df = centers_per_neigh_stats_df.merge(elder_with_access_df, left_index = True, right_index =True, how = 'outer')
    centers_per_neigh_stats_df = centers_per_neigh_stats_df.merge(severe_with_access_df, left_index = True, right_index =True, how = 'outer')
    centers_per_neigh_stats_df = centers_per_neigh_stats_df.reset_index()
    return centers_per_neigh_stats_df


if __name__ == "__main__":
    print('------------------- Runing hma0_merge.py -------------------')
    
    TESTING = True
    if TESTING:
        logging.basicConfig(stream=sys.stderr, level=logging.INFO)
        df_households_members_area = read_csv_idobject(f'data/testData/process/households_member_area_processed_test.csv')
        gdf_centers  = read_csv_idobject(f'data/testData/process/centers_processed.csv')
        gdf_municipality_processed = read_csv_idobject(f'data/testData/process/municipality_processed.csv', geodata=True)
        gdf_neighborhood_processed.read_csv_idobject(f'data/testData/process/neighborhood_processed.csv', geodata=True)

    else:
        logging.basicConfig(stream=sys.stderr, level=logging.ERROR)
        df_households_members_area = read_csv_idobject(f'data/pipeline/process/households_member_area_processed_pipeline.csv')
        gdf_centers = read_csv_idobject(f'data/testData/process/centers_processed.csv')
        gdf_municipality_processed = read_csv_idobject(f'data/pipeline/process/municipality_processed.csv', geodata=True)
        gdf_neighborhood_processed.read_csv_idobject(f'data/pipeline/process/neighborhood_processed.csv', geodata=True)

    centers_areas_walk = gpd.read_file( f'data/raw/centers_areas_walk.geojson', encoding = 'utf-8')
    centers_areas_walk = gpd.GeoDataFrame(centers_areas_walk, crs="EPSG:4326", geometry='geometry')
    centers_areas_walk = centers_areas_walk[['id','geometry']]

    centers_areas_car = gpd.read_file( f'data/raw/centers_areas_car.geojson', encoding = 'utf-8')
    centers_areas_car = gpd.GeoDataFrame(centers_areas_car, crs="EPSG:4326", geometry='geometry')
    centers_areas_car =centers_areas_car[['id','geometry']]
    #Dissolve influence areas
    centers_areas_car_walk = dissolve_dataframe_areas(centers_areas_walk, centers_areas_car)

    print('♑ ---------- 07_01 STATS Municipality---------------------             ')
    #child_with_access_neigh_df           
    df_child_stats_municipality = get_child_stats(df_households_members_area,  key_col='municipality_key')
    #elder_with_access_neigh_df, severe_with_access_neigh_df
    df_elder_stats_municipality, df_severe_stats_municipality = get_elder_severe_stats(df_households_members_area,  key_col='municipality_key')
    #capacity_per_mun_df
    #capacity_per_mun_df, centers_per_neigh_df
    df_cap_assigned_municipality  = get_centers_stats(gdf_centers, gdf_municipality_processed, centers_areas_car_walk ,  key_col='municipality_key')
    #centers_per_mun_stats_df
    df_stats_processed_municipality =  join_stats(df_cap_assigned_municipality, df_child_stats_municipality, df_elder_stats_municipality, df_severe_stats_municipality)

    if TESTING:
        df_stats_processed_municipality.to_csv(f'data/testData/process/stats_processed_municipality_test.csv', index = False)
    else:
        df_stats_processed_municipality.to_csv(f'data/pipeline/process/stats_processed_municipality_pipeline.csv', index = False)

    print('♑ ---------- 07_02 STATS Neighborhoods---------------------             ')
    #child_with_access_neigh_df           
    df_child_stats_neighborhood = get_child_stats(df_households_members_area, key_col='neighborhood_key')
    #elder_with_access_neigh_df, severe_with_access_neigh_df
    df_elder_stats_neighbohood, df_severe_stats_neighborhood = get_elder_severe_stats(df_households_members_area, key_col='neighborhood_key')
    #capacity_per_neigh_df
    #capacity_per_neigh_df, centers_per_neigh_df
    df_cap_assigned_neighborhood  = get_centers_stats(gdf_centers, gdf_neighborhood_processed, centers_areas_car_walk, key_col='neighborhood_key')
    #centers_per_neigh_stats_df
    df_stats_processed_neighborhood =  join_stats(df_cap_assigned_neighborhood, df_child_stats_neighborhood, df_elder_stats_neighbohood, df_severe_stats_neighborhood)

    if TESTING:
        df_stats_processed_neighborhood.to_csv(f'data/testData/process/stats_processed_neighborhood_test.csv', index = False)
    else:
        df_stats_processed_neighborhood.to_csv(f'data/pipeline/process/stats_processed_neighborhood_pipeline.csv', index = False)
