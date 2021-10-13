import numpy.testing as npt 
import pandas as pd 
import numpy as np 
import geopandas as gpd
from projectetl.utils.config import data_dir
from shapely import wkt

#Stage 00: AdminDiv process
from admindiv.adm0_process import process_admindiv

#Stage 001: Centers and Areas
from centers.cen0_process import process_centers, influenceareas_process

#Stage 02: Household process
from households.hou0_preprocess import (
          household_replace_transformer, 
          household_name_transfomrer,
          create_municipality_neighborhood_keys,
          bernoulli_imputation
          )

#Stage 03: Memebers preproecess
from members.mem0_preprocess import (
          members_name_transfomer,
          members_date_transfomer,
          member_assign_transformer,
          members_aggregate_transformer)

#Stage 04: Merge Households and Members
from merge_household_member.mhm0_merge import (
          mem_hous_merge_transformer,
          mem_hous_merge_select_transformer,
          mem_hous_merge_assign_trasnformer,
          mem_hous_merge_drop_transformer,
          )
#Stage 05: Superte by GeoDataFrame
from admindiv_aggroupation.adm0_superate import(get_admindiv_aggroupation)
 #Stage 07_02 &  Stage 07_1: STATS Neighborhood Municipality
from stats.sta0_createstats import (
      get_child_stats,
      get_elder_severe_stats,
      get_centers_stats,
      join_stats,
)
#Stage 04: Merge & Neighborhoods
from metrics.met0_createindex import (
      condense_admindiv,
      get_coverage_offer_indexes,
      get_percentages_demand_indexes,
)
#Stage 04: Merge & Neighborhoods
from merge_household_member_area.hma0_merge import (
          mem_hous_merge_name_transformer,
          to_geodata,
          merge_mem_hous_areas,
          )

from export.exp0_export import(
    export_name_transformer,
    format_varibles
)

from utils.u0_utils import dissolve_dataframe_areas


def test_dict(internal_dict, external_dict, p):
    cont =0
    for k in internal_dict.keys():
        if k in external_dict.keys():
            if external_dict[k] ==0:
                diff = internal_dict[k]
            else:
                diff = np.abs((internal_dict[k]-external_dict[k])/ external_dict[k])
                if diff>.1:
                    cont +=1
                    print(f"\n\n---Significative difference at: {k} \n\t internal_dict: {internal_dict[k]} \n\t external_dict: {external_dict[k]}")
    print(f'CONT: {cont} / {len(internal_dict)} variables with significative difference')

import logging, sys


TESTING = False 
NEW_CENTERS = True

if TESTING:
    #More prints
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
else:
    #No prints only process update
    logging.basicConfig(stream=sys.stderr, level=logging.ERROR)



print('# --------- RAW DATA -------------')
#Administrative Division
gdf_municipalities = gpd.read_file( f'data/raw/municipalities/municipalities_raw.shp', encoding = 'utf-8')
gdf_neighborhood = gpd.read_file(f'data/raw/neighborhoods/neighborhoods_raw.shp')
#Administrative Division - additions
df_provinces_codes = pd.read_excel(f'data/raw/province_codes.xls',skiprows=6, skipfooter=46).dropna(how='all', axis=1)
gdf_caipi = gpd.read_file(f'data/raw/caipi_raw.geojson', encoding = 'utf-8')
centers_areas_walk = gpd.read_file( f'data/raw/centers_areas_walk.geojson', encoding = 'utf-8')
centers_areas_car = gpd.read_file( f'data/raw/centers_areas_car.geojson', encoding = 'utf-8')


print('♈ ---------- 00 ADMINDIV PROCESS -------------')
gdf_neighborhood_processed, gdf_municipality_processed = process_admindiv(gdf_municipalities, gdf_neighborhood, df_provinces_codes, gdf_caipi)

if TESTING:
    gdf_neighborhood_processed.to_csv(f'data/testData/process/neighborhood_processed.csv', index = False)
    gdf_municipality_processed.to_csv(f'data/testData/process/municipality_processed.csv', index = False)

else:
    gdf_neighborhood_processed.to_csv(f'data/pipeline/process/neighborhood_processed.csv', index = False)
    gdf_municipality_processed.to_csv(f'data/pipeline/process/municipality_processed.csv', index = False)


print('♉ ---------- 01 CENTERS & AREAS PROCESS/LOAD------------')
#In this section we are loading gdf_areas, gdf_ceters and gdf_walk_car_areas_dissolved which is processed either by the functions process_centers
#and influenceareas_process or by the jupyter netobook process_centers.ipynb. The processed files are a single csv file 
# that can easily be modified instead of processing all the information from the raw files.
#load Data
#gdf_areas, gdf_centers = process_centers(gdf_municipality_processed, gdf_neighborhood_processed)
#gdf_areas_processed, gdf_walk_car_areas_dissolved = influenceareas_process(centers_areas_walk, centers_areas_car, gdf_areas)

#if TESTING:
#    gdf_areas_processed.to_csv(f'data/testData/process/areas_processed.csv', index = False)
#    gdf_centers.to_csv(f'data/testData/process/centers_processed.csv', index = False)

#else:
#    gdf_areas_processed.to_csv(f'data/testData/process/areas_processed.csv', index = False)
#    gdf_centers.to_csv(f'data/testData/process/centers_processed.csv', index = False)

gdf_areas_processed = pd.read_csv('data/pipeline/process/areas_processed.csv')
gdf_areas_processed['geometry'] = gdf_areas_processed['geometry'].apply(wkt.loads)
gdf_areas_processed = gpd.GeoDataFrame(gdf_areas_processed, crs="EPSG:4326", geometry='geometry')

gdf_centers = pd.read_csv('data/pipeline/process/centers_processed.csv')
gdf_centers['geometry'] = gdf_centers['geometry'].apply(wkt.loads)
gdf_centers = gpd.GeoDataFrame(gdf_centers, crs="EPSG:4326", geometry='geometry')

gdf_centers_new =  pd.read_csv('data/raw/newcenters_raw.csv')
gdf_centers_new['geometry'] = gdf_centers_new['geometry'].apply(wkt.loads)
gdf_centers_new = gpd.GeoDataFrame(gdf_centers_new, crs="EPSG:4326", geometry='geometry')
if NEW_CENTERS:
    gdf_centers = gdf_centers.append(gdf_centers_new)

gdf_walk_car_areas_dissolved = pd.read_csv('data/pipeline/process/walk_car_areas_dissolved.csv')
gdf_walk_car_areas_dissolved['geometry'] = gdf_walk_car_areas_dissolved['geometry'].apply(wkt.loads)
gdf_walk_car_areas_dissolved = gpd.GeoDataFrame(gdf_walk_car_areas_dissolved, crs="EPSG:4326", geometry='geometry')


print('♉ ---------- 01_02 LOAD SIUBEN------------')
if  TESTING: 
    df_members = pd.read_csv(f'data/raw/members_raw_test.csv')
    df_households =pd.read_csv(f'data/raw/households_raw_test.csv')
else:
    #Household and members all data
    #df_members = pd.read_csv(f'data/raw/members_raw.csv')
    df_households =pd.read_csv(f'data/raw/households_raw.csv')
    df_members = pd.read_csv(f'data/raw/members_raw.csv')
    
print('♊----------- 02 HOUSEHOLDS-------------')

#household_rt_out
df_households = household_replace_transformer(df_households)
#household_df
df_households  = household_name_transfomrer(df_households)
#households_ast_out
df_households = create_municipality_neighborhood_keys(df_households)
#bss_out -> superate_at_out
df_households_processed = bernoulli_imputation(df_households)

if TESTING:
    df_households_processed.to_csv(f'data/testData/process/households_processed_test.csv', index = False)
else:
     df_households_processed.to_csv(f'data/pipeline/process/households_processed_pipeline.csv', index = False)
   
print('♋ ---------- 03 MEMBERS-------------')
#miembros_df 
df_members = members_name_transfomer(df_members)
#ExtraStep
df_members = members_date_transfomer(df_members)
#members_ast_out
df_members = member_assign_transformer(df_members)
#sample_agg_miembros_df
df_members_processed = members_aggregate_transformer(df_members)

if TESTING:
    df_members_processed.to_csv(f'data/testData/process/members_processed_test.csv', index = False)
else:
     df_members_processed.to_csv(f'data/pipeline/process/members_processed_pipeline.csv', index = False)


print('♌ ---------- 04 MERGE HOUSEHOLD MEMBER ----------')
#merged_df
df_households_members = mem_hous_merge_transformer(df_households_processed, df_members_processed)
#fst_out
df_households_members = mem_hous_merge_select_transformer(df_households_members)
#household_ast_out
df_households_members = mem_hous_merge_assign_trasnformer(df_households_members)
#superate_dt_out
df_households_members_processed = mem_hous_merge_drop_transformer(df_households_members)

if TESTING:
    df_households_members_processed.to_csv(f'data/testData/process/households_members_processed_test.csv', index = False)
else:
    df_households_members_processed.to_csv(f'data/pipeline/process/households_members_processed_pipeline.csv', index = False)

print('♍ ---------- 05_01 HOUSEHOLD MEMBER Administrative Agrrupation/Municipalities--------------------')
#neigh_dependance_df
df_municipality_grouped =get_admindiv_aggroupation(df_households_members_processed, key_col = 'municipality_key')

if TESTING:
    df_municipality_grouped.to_csv(f'data/testData/process/df_municipality_grouped_test.csv', index = False)
else:
    df_municipality_grouped.to_csv(f'data/pipeline/process/df_municipality_grouped_pipeline.csv', index = False)
   
print('♍ ---------- 05_02 HOUSEHOLD MEMBER Administrative Agrrupation/Neighborhood--------------------')
#neigh_dependance_df
df_neighborhood_grouped =get_admindiv_aggroupation(df_households_members_processed, key_col = 'neighborhood_key')

if TESTING:
    df_neighborhood_grouped.to_csv(f'data/testData/process/df_neighborhood_grouped_test.csv', index = False)
else:
    df_neighborhood_grouped.to_csv(f'data/pipeline/process/df_neighborhood_grouped_pipeline.csv', index = False)
   
print('♎ ---------- 06 MERGE HOUSEHOLD MEMBER AREA ----------')

#src_superate_gdf
#Note that we are not renaming df_households_members_processed because we are going to use it again in get_neighbohood_dependance
df_households_members_processed = mem_hous_merge_name_transformer(df_households_members_processed) 
#superate_gdf 
df_households_members_processed = to_geodata(df_households_members_processed, 'lat', 'lon', 'geometry')
#merged_superate_gdf
df_households_members_area_processed = merge_mem_hous_areas(df_households_members_processed, gdf_areas_processed)

if TESTING:
    df_households_members_area_processed.to_csv(f'data/testData/process/households_member_area_processed_test.csv', index = False)
else:
     df_households_members_area_processed.to_csv(f'data/pipeline/process/households_member_area_processed_pipeline.csv', index = False)
   

print('♏ ---------- 07_01 STATS Municipality---------------------             ')
#child_with_access_neigh_df           
df_child_stats_municipality = get_child_stats(df_households_members_area_processed,  key_col='municipality_key')
#elder_with_access_neigh_df, severe_with_access_neigh_df
df_elder_stats_municipality, df_severe_stats_municipality = get_elder_severe_stats(df_households_members_area_processed,  key_col='municipality_key')
#capacity_per_mun_df
#capacity_per_mun_df, centers_per_neigh_df
df_cap_assigned_municipality  = get_centers_stats(gdf_centers, gdf_municipality_processed, gdf_walk_car_areas_dissolved ,  key_col='municipality_key')
#centers_per_mun_stats_df
df_stats_processed_municipality =  join_stats(df_cap_assigned_municipality, df_child_stats_municipality, df_elder_stats_municipality, df_severe_stats_municipality)

if TESTING:
    df_stats_processed_municipality.to_csv(f'data/testData/process/stats_processed_municipality_test.csv', index = False)
else:
     df_stats_processed_municipality.to_csv(f'data/pipeline/process/stats_processed_municipality_pipeline.csv', index = False)

print('♏ ---------- 07_02 STATS Neighborhoods---------------------             ')
#child_with_access_neigh_df           
df_child_stats_neighborhood = get_child_stats(df_households_members_area_processed, key_col='neighborhood_key')
#elder_with_access_neigh_df, severe_with_access_neigh_df
df_elder_stats_neighbohood, df_severe_stats_neighborhood = get_elder_severe_stats(df_households_members_area_processed, key_col='neighborhood_key')
#capacity_per_neigh_df
#capacity_per_neigh_df, centers_per_neigh_df
df_cap_assigned_neighborhood  = get_centers_stats(gdf_centers, gdf_neighborhood_processed, gdf_walk_car_areas_dissolved, key_col='neighborhood_key')
#centers_per_neigh_stats_df
df_stats_processed_neighborhood =  join_stats(df_cap_assigned_neighborhood, df_child_stats_neighborhood, df_elder_stats_neighbohood, df_severe_stats_neighborhood)

if TESTING:
    df_stats_processed_neighborhood.to_csv(f'data/testData/process/stats_processed_neighborhood_test.csv', index = False)
else:
     df_stats_processed_neighborhood.to_csv(f'data/pipeline/process/stats_processed_neighborhood_pipeline.csv', index = False)


print('♑ ---------- 08_01 METRICS Municipality---------------')
#df_stats_neighborhoods
df_metrics_municipality = condense_admindiv(gdf_municipality_processed, df_municipality_grouped, df_stats_processed_municipality, key_col='municipality_key')
#neigh_icast_out
df_metrics_municipality = get_coverage_offer_indexes(df_metrics_municipality)

#neigh_past_out, neigh_cast_out
neigh_past_out, df_metrics_municipality_processed = get_percentages_demand_indexes(df_metrics_municipality)

if TESTING:
    df_metrics_municipality_processed.to_csv(f'data/testData/process/metrics_municipality_processed_test.csv', index = False)
else:
    df_metrics_municipality_processed.to_csv(f'data/pipeline/process/metrics_municipality_processed_pipeline.csv', index = False)


print('♑ ---------- 08_02 METRICS Neighborhoods---------------')
#df_stats_neighborhoods
df_metrics_neighborhoods = condense_admindiv(gdf_neighborhood_processed, df_neighborhood_grouped, df_stats_processed_neighborhood, key_col='neighborhood_key')
#neigh_icast_out
df_metrics_neighborhoods = get_coverage_offer_indexes(df_metrics_neighborhoods)
#neigh_past_out, neigh_cast_out
neigh_past_out, df_metrics_neighborhoods_processed = get_percentages_demand_indexes(df_metrics_neighborhoods)

if TESTING:
    df_metrics_neighborhoods_processed.to_csv(f'data/testData/process/metrics_neighborhoods_processed_test.csv', index = False)
else:
    df_metrics_neighborhoods_processed.to_csv(f'data/pipeline/process/metrics_neighborhoods_processed_pipeline.csv', index = False)
   
   
print('♒ ---------- 09_01 EXPORT---------------')
#prioritize_map_export_neigh_gdf
df_export_municipaily = export_name_transformer(df_metrics_municipality_processed, key_col ='municipality_key')
#final df_export_neigh
df_export_municipaily_processed =   format_varibles( df_export_municipaily, 3)

if TESTING:
    df_export_municipaily_processed.to_csv( f'data/testData/export/mepyd_municipality_20210930_test.csv', index = False)
else:
     df_export_municipaily_processed.to_csv( f'data/pipeline/export/mepyd_municipality_20210930.csv', index = False)


print('♒ ---------- 09_02 EXPORT---------------')
#prioritize_map_export_neigh_gdf
df_export_neighborhood = export_name_transformer(df_metrics_neighborhoods_processed, key_col = 'neighborhood_key')
#final df_export_neigh
df_export_neighborhood_processed =   format_varibles( df_export_neighborhood, 3)

if TESTING:
    df_export_neighborhood_processed.to_csv( f'data/testData/export/mepyd_neighborhoods_20210930_test.csv', index = False, encoding='utf-8')
else:
    df_export_neighborhood_processed.to_csv( f'data/pipeline/export/mepyd_neighborhoods_20210930.csv', index = False)
   
print('♓ ---------- 10 Export Influence Areas----------------')
#influence_area_stats
#df_areas_export = groupby_household(df_households_members_area_processed)
#area_map_gdf
#df_areas_export = recover_areas_information(df_areas_export, gdf_areas_processed)
#past_out
#df_areas_export = percentage_metrics(df_areas_export)
#cast_out
#df_areas_export_processed = index_metrics_creation(df_areas_export)

if TESTING:
    gdf_walk_car_areas_dissolved.to_csv(f'data/testData/export/mepyd_areas_20210930.csv', index = False)
    gdf_centers.to_csv(f'data/testData/export/mepyd_centers_20210930.csv', index = False)
else:
    gdf_walk_car_areas_dissolved.to_csv(f'data/pipeline/export/mepyd_areas_20210930.csv', index = False)
    gdf_centers.to_csv(f'data/pipeline/export/mepyd_centers_20210930.csv', index = False)
