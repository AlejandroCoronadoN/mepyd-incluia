import logging, sys
import pandas as pd 
import numpy as np 
import geopandas as gpd
from projectetl.utils.config import data_dir
from scipy.stats import hmean
from sklearn.preprocessing import MinMaxScaler
from tqdm import tqdm
from utils.u0_utils import  dissolve_dataframe_areas, create_index_metric
from criteriaetl.transformers.columns_base import (NameTransformer, 
    ReplaceTransformer, SelectTransformer, AssignTransformer)
import pdb

def condense_admindiv(gdf_admindiv, df_admindiv_processed, df_stats_processed, key_col):
    df_admindiv_processed = df_admindiv_processed.set_index(key_col)
    gdf_admindiv =  gdf_admindiv.set_index(key_col)
    df_stats_processed = df_stats_processed.set_index(key_col)
    
    # merge id, dependance and center stats columns
    gdf_admindiv_merge = gdf_admindiv.merge(
        df_admindiv_processed, right_index = True, left_index = True, how =  'left').merge(
        df_stats_processed, right_index = True, left_index = True, how='left').fillna({
        col: 0 for col in df_stats_processed.columns})

    # build human readable municipality-province column
    gdf_admindiv_merge.loc[:,'muni_province'] = gdf_admindiv_merge.Municipio.str.cat(
        gdf_admindiv_merge.Provincia, sep=', ')
    # build human readable neighborhood-municipality column
    if key_col == 'neighborhood_key':
        gdf_admindiv_merge.loc[:,'neigh_muni'] = gdf_admindiv_merge.TOPONIMIA.str.cat(
            gdf_admindiv_merge.Municipio, sep=', ')
    return gdf_admindiv_merge 

def get_coverage_offer_indexes(gdf_admindiv_merge):
    '''
    TODO: utilizamos 
    '''
    #demand_col = ['member_0to4_sum', 'superate_older_dependant_sum', 'superate_disability_5to64_sum', 'is_female_sum', 'member_id_count']
    #for col in demand_col:
    #    gdf_admindiv_merge[col] = gdf_admindiv_merge[col].replace(np.nan, 0)
    coverage_dict =  {
        'coverage_caipi': ['capacidad total (CAIPI)', 'member_0to4_sum' ],
        'coverage_ccpp_epes': ['capacidad total (CCPP CON EPES)', 'member_0to4_sum'],
        'coverage_ccpp_sin_epes':  [ '# CCPP SIN EPES', 'member_0to4_sum' ],
        'coverage_ctc_epes':   ['capacidad total (CTC CON EPES)', 'member_0to4_sum' ],
        'coverage_prosoli':  [ '# Direccion Regional PROSOLI', 'member_0to4_sum' ],
        'coverage_asfl_dia':   ['capacidad total (ASFL DIURNAS)',  'superate_older_dependant_sum' ],
        'coverage_asfl_permanente' :   ['capacidad total (ASFL PERMANENTES)',  'superate_older_dependant_sum' ],
        'coverage_conape_estancia': ['capacidad total (ESTANCIAS CONAPE)',  'superate_older_dependant_sum' ],
        'coverage_conape_dia' : ['capacidad total (Nuevos C. de Dia CONAPE)',  'superate_older_dependant_sum' ],
        'coverage_total_conape' :   ['capacidad total (TOTAL CONAPE)',  'superate_older_dependant_sum' ],
        'coverage_caid' :  [ '# CAID',  'superate_disability_5to64_sum' ],
        'coverage_mujer' :  [ '# Oficina de la Mujer',  'is_female_sum' ],
        'coverage_cos_infotep' :  [ '# COS INFOTEP',  'member_id_count' ],
        'coverage_infotep' :  [ '# Direccion Regional INFOTEP',  'member_id_count' ]
    }
    for k in tqdm(coverage_dict.keys(), 'get_coverage_offer_indexes: '):
        coverage_var = k
        numerator =  coverage_dict[k][0]
        denominator = coverage_dict[k][1]
        gdf_admindiv_merge_n0 = gdf_admindiv_merge[gdf_admindiv_merge[denominator] >0]
        max_cover = np.max(gdf_admindiv_merge_n0[numerator] /gdf_admindiv_merge_n0[denominator])
        
        gdf_admindiv_merge[coverage_var] = 0
        
        gdf_admindiv_merge.loc[ (gdf_admindiv_merge[denominator]== 0) & (gdf_admindiv_merge[numerator]> 0), coverage_var ] = max_cover
        gdf_admindiv_merge.loc[ (gdf_admindiv_merge[denominator].isna())&((gdf_admindiv_merge[numerator]> 0) ), coverage_var ] = max_cover
        
        gdf_admindiv_merge.loc[ gdf_admindiv_merge[numerator].isna(), coverage_var ] = 0

        gdf_admindiv_merge.loc[ (gdf_admindiv_merge[denominator] > 0)&( ~gdf_admindiv_merge[numerator].isna()), coverage_var ] = gdf_admindiv_merge_n0[numerator]/gdf_admindiv_merge_n0[denominator]
        q95 = gdf_admindiv_merge[coverage_var].quantile(.95)
        if q95>0:
            gdf_admindiv_merge.loc[(gdf_admindiv_merge[coverage_var] > gdf_admindiv_merge[coverage_var].quantile(.95)), coverage_var] = q95
        gdf_admindiv_merge[ coverage_var ] = (gdf_admindiv_merge[ coverage_var ] - gdf_admindiv_merge[ coverage_var ].min())/gdf_admindiv_merge[ coverage_var ].max()
        print(f'\n\n ** get_coverage_offer_indexes/coverage_var: {coverage_var} \n\tmax_cover: {max_cover} \n\tmean: {gdf_admindiv_merge[coverage_var].mean()}')
    df_coverage_offer = gdf_admindiv_merge.copy()
    
    ici_weights = {'coverage_caipi' : 4,
        'coverage_ccpp_epes' : 1,
        'coverage_ccpp_sin_epes' : 1,
        'coverage_ctc_epes' : 1,
        'coverage_prosoli' : 1,
        'coverage_total_conape' : 4,
        'coverage_caid' : 1,
        'coverage_mujer' : 1,
        'coverage_cos_infotep' : 1,
        'coverage_infotep' : 1}

    ioferta_weights = {
        #children 0 to 4 years
        'capacidad total (CAIPI)':4,
        'capacidad total (CCPP CON EPES)':1,
        '# CCPP SIN EPES':1,
        'capacidad total (CTC CON EPES)':1,
        '# Direccion Regional PROSOLI':1,
        #superate_older_dependant_sum'
        'capacidad total (TOTAL CONAPE)':4,
        #'superate_disability_5to64_sum'
        '# CAID':1,
        '# Oficina de la Mujer':1,
        #'member_id_count':[
        '# COS INFOTEP':1,
        '# Direccion Regional INFOTEP':1
    }
    df_coverage_offer = create_index_metric(df_coverage_offer, ici_weights, 'installed_cap_index' )
    df_coverage_offer = create_index_metric(df_coverage_offer, ioferta_weights, 'offer_index' )           
            
    return df_coverage_offer  




def get_percentages_demand_indexes(df_coverage_offer):
    color_tuples = [
    ('child_0to4', 'dependence_0to4_rate'),
    ('child_5to12', 'dependence_5to12_rate'),
    ('disability_5to64', 'dependence_dissability_5to64_rate'),
    ('disability_13to64', 'dependence_dissability_13to64_rate'),
    ('older_dependant', 'dependence_older_dependant_rate'),
    ('older_severe_dependant', 'dependence_older_severe_dependant_rate'),
    ('care_receiver', 'dependence_rate_micro'),
    ('care_receiver_wo_mild_dependant', 'dependence_rate_micro_wo_mild_dependant'),
    ('care_receiver_wo_5to12', 'dependence_rate_micro_wo_5to12'),
    ('care_receiver_wo_both', 'dependence_rate_micro_wo_both'),
    ]

    color_assign_map = {
        f'agg_dependence_rate_{suffix}': lambda df, ratio=ratio_col,
        percentage=f'percentage_households_{suffix}': df[ratio].mul(
            df[percentage]) for suffix, ratio_col in 
            color_tuples
    }
    output_assign_map = {
        **color_assign_map,

        # care prioritization index
        'demanda_de_ciudado': lambda df: np.nanmean(MinMaxScaler().fit_transform(df[[
            'agg_dependence_rate_child_0to4', 
            'agg_dependence_rate_disability_5to64',
            'agg_dependence_rate_older_dependant'
        ]]), axis=1),
        'presencia_de_ciudadoras': lambda df: np.nanmean(MinMaxScaler().fit_transform(df[[
            'percentage_woman_houseworker', 
            'percentage_households_monomarental_child',
            'percentage_households_monomarental_disability',
            'percentage_households_monomarental_elder_dependant',
        ]]), axis=1),
        'pobreza': lambda df: np.nanmean(MinMaxScaler().fit_transform(df[[
            'percentage_poor_women',
            'percentage_households_poor'
        ]]), axis=1),
        'indice_priorizacion_ciudados': lambda df: np.nanmean(MinMaxScaler().fit_transform(df[[
            'demanda_de_ciudado', 
            'presencia_de_ciudadoras',
            'pobreza',
            
        ]]), axis=1),
        
        'indice_agregado_demanda': lambda df: np.nanmean(MinMaxScaler().fit_transform(df[[
            'superate_child_0to4_receiver_sum', 
            'superate_disability_5to64_sum',
            'superate_older_dependant_sum',
            'woman_poor_sum',
            'is_poor',
            'woman_houseworker_sum',
            'monomarental_with_superate_child',
            'monomarental_with_superate_elder_dependant',
            'monomarental_with_superate_disability',
        ]]), axis=1),
    }
    #--------------------------------------
    
    #TODO: Cambiar esta mamarrachada y poner el diccionario de manera legible
    percentage_tuples = [
    ('child_0to4', 'superate_child_0to4_receiver_any'),
    ('child_5to12', 'superate_child_5to12_receiver_any'),
    ('disability_5to64', 'superate_disability_5to64_any'),
    ('disability_13to64', 'superate_disability_13to64_any'),
    ('older_dependant', 'superate_older_dependant_any'),
    ('older_severe_dependant', 'superate_older_severe_dependant_any'),
    ('care_receiver', 'superate_care_receiver_any'),
    ('care_receiver_wo_mild_dependant', 'superate_care_receiver_wo_mild_dependant_any'),
    ('care_receiver_wo_5to12', 'superate_care_receiver_wo_5to12_any'),
    ('care_receiver_wo_both', 'superate_care_receiver_wo_both_any'),
    ('domestic_worker', 'domestic_worker_any'),
    ('monomarental_child', 'monomarental_with_superate_child'),
    ('monomarental_disability', 'monomarental_with_superate_disability'),
    ('monomarental_elder_dependant', 'monomarental_with_superate_elder_dependant'),
    ('poor', 'is_poor'),
    ]
    percentage_assign_map = {
        # number of households as denominator
        **{
            f'percentage_households_{suffix}': lambda df, numerator=receiver_col: 
            df[numerator].div(df['household_id']) for suffix, receiver_col in 
            percentage_tuples
        },
        # women in work age as denominator
        **{
            f'percentage_{col_root}': lambda df, numerator=f'{col_root}_sum': df[
                numerator].div(df['woman_work_age_sum']) for col_root in [
                'woman_domestic_worker', 'woman_houseworker'
            ]
        },
        'percentage_men_domestic_worker': lambda df: df[
            'man_domestic_worker_sum'].div(df['man_work_age_sum']),
        'percentage_poor_women': lambda df: df[
            'woman_poor_sum'].div(df['is_female_sum'])
    }
    
    #Aditional information for future development
    neigh_percentage_assign_transformer = AssignTransformer({
        **percentage_assign_map,
        **{f'perc_{col}': lambda df, numerator=col: df[numerator].div(df[
            'superate_child_0to4_receiver_sum']) for col in [
            'accesible_child_0to4_receiver_caipi', 
            'accesible_child_0to4_receiver_ccpp_epes'
        ]},
        **{f'perc_{col}': lambda df, numerator=col: df[numerator].div(df[
            'superate_older_dependant_sum']) for col in [
            'accesible_elder_asfl_diurna', 'accesible_elder_asfl_permanente', 
            'accesible_elder_estancias_conape', 'accesible_elder_nuevos_conape'
        ]},
        **{f'perc_{col}': lambda df, numerator=col: df[numerator].div(df[
            'superate_older_severe_dependant_sum']) for col in [
            'accesible_severe_asfl_diurna', 'accesible_severe_asfl_permanente', 
            'accesible_severe_estancias_conape', 'accesible_severe_nuevos_conape'
        ]}
    })
    df_coverage_offer_percentage = neigh_percentage_assign_transformer.transform(df_coverage_offer)
    # We take output_assign_transformer from previous code and use it here.
    output_assign_transformer = AssignTransformer(output_assign_map)
    df_coverage_offer_percentage_demand = output_assign_transformer.transform(df_coverage_offer_percentage)
    return df_coverage_offer_percentage, df_coverage_offer_percentage_demand



if __name__ == "__main__":
    print('------------------- Runing met0_createindex.py -------------------')\
    
    TESTING = True
    if TESTING:
        #More prints
        logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    else:
        #No prints only process update
        logging.basicConfig(stream=sys.stderr, level=logging.ERROR)

    cols_municipalities_gpd = [
                'Provincia', 'Municipio', 'municipality_key', 'caipi_sum',
                'geometry'
        ]
    cols_neighborhood_gpd = [
                'Provincia', 'TOPONIMIA', 'neighborhood_key', 'caipi_sum_neighbor',
                'geometry','municipality_key', 'Municipio'
        ]
    #Neighborhoods
    neigh_geojson_path = (f'data/preprocess/preprocess_neighborhood.geojson')
    gdf_neighborhood = gpd.read_file(neigh_geojson_path)[cols_neighborhood_gpd].copy()
    gdf_neighborhood['TOPONIMIA'] = gdf_neighborhood['TOPONIMIA'].str.title()

    #Municipalities
    #TODO: Since we are using the same municipality, neighbor and areas files for both testing and not testing.
    munis_geojson_path = (f'data/preprocess/preprocess_municipality.geojson')
    gdf_municipality = gpd.read_file(munis_geojson_path)[cols_municipalities_gpd].copy()
    
    df_neighborhood_grouped = pd.read_csv(f'data/testData/process/df_neighborhood_grouped_test.csv' )
    df_municipality_grouped = pd.read_csv(f'data/testData/process/df_municipality_grouped_test.csv' )
    df_stats_processed_municipality = pd.read_csv(f'data/testData/process/stats_processed_municipality_test.csv' )
    df_stats_processed_neighborhood = pd.read_csv(f'data/testData/process/stats_processed_neighborhood_test.csv')

    #---------- 08_01 METRICS Municipality---------------
    #df_stats_neighborhoods
    df_metrics_municipality = condense_admindiv(gdf_municipality, df_municipality_grouped, df_stats_processed_municipality, key_col='municipality_key')
    #neigh_icast_out
    df_metrics_municipality = get_coverage_offer_indexes(df_metrics_municipality)
    #neigh_past_out, neigh_cast_out
    neigh_past_out, df_metrics_municipality_processed = get_percentages_demand_indexes(df_metrics_municipality)

    df_metrics_municipality_processed.to_csv(f'data/testData/process/metrics_municipality_processed_test.csv', index = False)


    #---------- 08_02 METRICS Neighborhoods---------------
    #df_stats_neighborhoods
    df_metrics_neighborhoods = condense_admindiv(gdf_neighborhood, df_neighborhood_grouped, df_stats_processed_neighborhood, key_col='neighborhood_key')
    #neigh_icast_out
    df_metrics_neighborhoods = get_coverage_offer_indexes(df_metrics_neighborhoods)
    #neigh_past_out, neigh_cast_out
    neigh_past_out, df_metrics_neighborhoods_processed = get_percentages_demand_indexes(df_metrics_neighborhoods)

    df_metrics_neighborhoods_processed.to_csv(f'data/testData/process/metrics_neighborhoods_processed_test.csv', index = False)
