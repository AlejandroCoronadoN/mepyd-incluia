import pandas as pd 
import numpy as np 
import geopandas as gpd
from scipy.stats import hmean

def get_admindiv_aggroupation(superate_dt_out, key_col):
    ''''''
    hmean_cols = ['dependence_0to4_rate',
        'dependence_5to12_rate',
        'dependence_dissability_13to64_rate',
        'dependence_dissability_5to64_rate',
        'dependence_older_dependant_rate',
        'dependence_older_severe_dependant_rate',
        'dependence_rate_micro_wo_both',
        'dependence_rate_micro_wo_5to12',
        'dependence_rate_micro_wo_mild_dependant',
        'dependence_rate_micro' ]     
    
    agg_stats_dict = {'member_0to4_sum': 'sum',
        'recibe_ciudados_inaipi_sum': 'sum',
        'recibe_ciudados_publico_no_inaipi_sum': 'sum',
        'recibe_ciudados_privado_sum': 'sum',
        'recibe_ciudados_otros_sum': 'sum',
        'superate_child_0to4_receiver_sum': 'sum',
        'member_5to12_sum': 'sum',
        'superate_child_5to12_receiver_sum': 'sum',
        'superate_disability_13to64_sum': 'sum',
        'superate_disability_5to64_sum': 'sum',
        'superate_older_dependant_sum': 'sum',
        'superate_older_severe_dependant_sum': 'sum',
        'woman_domestic_worker_sum': 'sum',
        'woman_work_age_sum': 'sum',
        'man_domestic_worker_sum': 'sum',
        'man_work_age_sum': 'sum',
        'member_id_count': 'sum',
        'superate_child_0to4_receiver_any': 'sum',
        'superate_child_5to12_receiver_any': 'sum',
        'superate_disability_13to64_any': 'sum',
        'superate_disability_5to64_any': 'sum',
        'superate_older_dependant_any': 'sum',
        'superate_older_severe_dependant_any': 'sum',
        'superate_care_receiver_any': 'sum',
        'superate_care_receiver_wo_mild_dependant_any': 'sum',
        'superate_care_receiver_wo_5to12_any': 'sum',
        'superate_care_receiver_wo_both_any': 'sum',
        'domestic_worker_any': 'sum',
        'woman_houseworker_sum': 'sum',
        'woman_poor_sum': 'sum',
        'is_female_sum': 'sum',
        'monomarental_with_superate_child': 'sum',
        'monomarental_with_superate_disability': 'sum',
        'monomarental_with_superate_elder_dependant': 'sum',
        'is_poor': 'sum',
        'household_id': 'count',
         **{ col: lambda srs: hmean(srs.replace(0, np.nan).dropna()) for col in hmean_cols },
         
        }
    neigh_key = 'neighborhood_key'

    superate_households_col = 'household_id'
    members_sum_col = 'member_id_count'
    superate_by_neigh = superate_dt_out.groupby(key_col, as_index=False)
    neigh_dependance_df = superate_by_neigh.agg(agg_stats_dict)
    
    return neigh_dependance_df

#DEPRECATED FUNCTION TODO: Delete
def get_neighbohood_gdf(neigh_dependance_df, neighborhood_gdf, centers_per_neigh_stats_df):
    neigh_dependance_df = neigh_dependance_df.set_index('neighborhood_key')
    neighborhood_gdf =  neighborhood_gdf.set_index('neighborhood_key')

    # merge id, dependance and center stats columns
    neigh_dependance_gdf = neighborhood_gdf.merge(
        neigh_dependance_df, right_index = True, left_index = True, how =  'left').merge(
        centers_per_neigh_stats_df, right_index = True, left_index = True, how='left').fillna({
        col: 0 for col in centers_per_neigh_stats_df.columns})
    # build human readable municipality-province column
    hover_title_col = 'muni_province'
    neigh_dependance_gdf.loc[:,hover_title_col] = neigh_dependance_gdf.Municipio.str.cat(
        neigh_dependance_gdf.Provincia, sep=', ')
    hover_title_col = 'neigh_muni'
    neigh_dependance_gdf.loc[:,hover_title_col] = neigh_dependance_gdf.TOPONIMIA.str.cat(
        neigh_dependance_gdf.Municipio, sep=', ')

    return neigh_dependance_gdf