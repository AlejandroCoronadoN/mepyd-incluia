from os import read
import pandas as pd 
import numpy as np 
import logging, sys
from tqdm import tqdm 

from criteriaetl.utils.common_func import (get_weighted_complete_randomization_series_on_subset, 
    proportional_cut, weighted_qcut, get_partition_bool_columns_dict)
from criteriaetl.transformers.columns_base import (NameTransformer, 
    ReplaceTransformer, SelectTransformer, AssignTransformer)
from criteriaetl.transformers.rows_base import (
    AggregateTransformer, DropTransformer)
from criteriaetl.transformers.fusion_base import MergeTransformer


def export_name_transformer(neigh_cast_out, key_col):
    prioritize_map_rename_dict = {
    #'neigh_muni': 'barrio',
    'muni_province': 'municipio',
    # 'dependence_0to4_rate': 'tasa de dependencia (0 a 4)',
    # 'dependence_dissability_rate': 'tasa de dependencia (discapacitados)', 
    # 'dependence_older_dependant_rate': 'tasa de dependencia (envejecientes)', 
    # 'dependence_rate_micro': 'tasa de dependencia total', 
    # 'member_0to4_sum': '# de personas de 0 a 4',
    # 'superate_disability_sum': '# de discapacitados',
    # 'superate_older_dependant_sum': '# de adultos mayores dependientes',
    # 'member_sum': '# personas superate',
    # 'household_sum': '# hogares superate',
    # 'caipi_sum': '# de CAIPI',
    'dependence_0to4_rate': 'ratio parcial de dependencia (0 a 4)',
    'percentage_households_child_0to4': 'porcentage hogares 0 a 4',
    'agg_dependence_rate_child_0to4': 'ratio total de dependencia (0 a 4)',
    'dependence_5to12_rate': 'ratio parcial de dependencia (5 a 12)',
    'percentage_households_child_5to12': 'porcentage hogares (5 a 12)',
    'agg_dependence_rate_child_5to12': 'ratio total de dependencia (5 a 12)',
    'dependence_dissability_5to64_rate': 'ratio parcial de dependencia (discapacitados 5 a 64)',
    'percentage_households_disability_5to64': 'porcentage hogares discapacitados 5 a 64',
    'agg_dependence_rate_disability_5to64': 'ratio total de dependencia (discapacitados 5 a 64)',
    'dependence_dissability_13to64_rate': 'ratio parcial de dependencia (discapacitados 13 a 64)',
    'percentage_households_disability_13to64': 'porcentage hogares discapacitado 13 a 64',
    'agg_dependence_rate_disability_13to64': 'ratio total de dependencia (discapacitados 13 a 64)',
    'dependence_older_dependant_rate': 'ratio parcial de dependencia(envejecientes)',
    'percentage_households_older_dependant': 'porcentage hogares envejecientes',
    'agg_dependence_rate_older_dependant': 'ratio total de dependencia (envejecientes)',
    'dependence_older_severe_dependant_rate': 'ratio parcial de dependencia grave (envejecientes)',
    'percentage_households_older_severe_dependant': 'porcentage hogares envejecientes dependencia grave',
    'agg_dependence_rate_older_severe_dependant': 'ratio total de dependencia grave (envejecientes)',
    'dependence_rate_micro': 'ratio parcial de dependencia agregada',
    'percentage_households_care_receiver': 'porcentage hogares dependencia agregada',
    'agg_dependence_rate_care_receiver': 'ratio total de dependencia agregada',
    'dependence_rate_micro_wo_5to12': 'ratio parcial de dependencia agregada sin 5 a 12',
    'percentage_households_care_receiver_wo_mild_dependant': 'porcentage hogares dependencia agregada sin 5 a 12',
    'agg_dependence_rate_care_receiver_wo_mild_dependant': 'ratio total de dependencia agregada sin 5 a 12',
    'dependence_rate_micro_wo_mild_dependant': 'ratio parcial de dependencia agregada sin env dep leves',
    'percentage_households_care_receiver_wo_5to12': 'porcentage hogares dependencia agregada sin env dep leves',
    'agg_dependence_rate_care_receiver_wo_5to12': 'ratio total de dependencia agregada sin env dep leves',
    'dependence_rate_micro_wo_both': 'ratio parcial de dependencia agregada sin 5 a 12 ni env dep leves',
    'percentage_households_care_receiver_wo_both': 'porcentage hogares dependencia agregada sin 5 a 12 ni env dep leves',
    'agg_dependence_rate_care_receiver_wo_both': 'ratio total de dependencia agregada sin 5 a 12 ni env dep leves total',
    'member_0to4_sum': '# de personas de 0 a 4',
    
    'recibe_ciudados_inaipi_sum': '# 0 a 4 en CAIPI',
    'recibe_ciudados_publico_no_inaipi_sum': '# de 0 a 4 en otros públicos',
    'recibe_ciudados_privado_sum': '# de 0 a 4 en privados',
    'recibe_ciudados_otros_sum': '# de 0 a 4 en otros',
    'member_5to12_sum': '# de personas de 5 a 12',
    'superate_child_5to12_receiver_sum': '# de 5 a 12 sin s c ni tanda escolar completa',
    'superate_disability_13to64_sum': '# de discapacitados (13 a 64)',
    'superate_older_severe_dependant_sum': '# de adultos mayores dependientes graves',
    'member_id_count': '# personas superate',
    'superate_child_0to4_receiver_any': '# hogares con miembros de 0 a 4 sin s c ni tanda escolar completa',
    'superate_child_5to12_receiver_any': '# hogares con miembros de 5 a 12 sin s c ni tanda escolar completa',
    'superate_disability_5to64_any': '# hogares con miembros discapacitados (5 a 64)',
    'superate_disability_13to64_any': '# hogares con miembros discapacitados (13 a 64)',
    'superate_older_dependant_any': '# hogares con miembros adultos mayores dependientes',
    'superate_older_severe_dependant_any': '# hogares con miembros adultos mayores dependientes graves',
    'domestic_worker_any': '# de hogares con trabajadores domésticos',
    'percentage_households_domestic_worker': 'porcentage hogares con trabajadores domésticos',
    'woman_work_age_sum': '# de mujeres en edad de trabajar',
    'percentage_woman_domestic_worker': 'porcentage de mujeres trabajadoras domésticas',
    'man_domestic_worker_sum': '# de trabajadores domésticos',
    'man_work_age_sum': '# de hombres en edad de trabajar',
    'percentage_men_domestic_worker': 'porcentage de hombres trabajadores domésticos',
    'demanda_de_ciudado': 'idc-demanda de ciudado',
    'presencia_de_ciudadoras': 'idc-presencia de ciudadoras',
    'pobreza': 'idc-pobreza',
    'indice_priorizacion_ciudados': 'indice de priorizacion de demanda',
    'indice_agregado_demanda': 'indice de demanda',

    # 'superate_child_0to4_receiver_reachable': 
    #     '# de personas con acceso en transporte de 0 a 4 ',
    # 'prop_member_0to4_reachable': 'porcentage personas de 0 a 4 con acceso en transporte',
    # 'dependence_0to4_reachable': 'ratio de dependencia (con acceso en '
    #     'transporte de 0 a 4)',
    
    #TODO: decompose function to get # CAIPI # CCPP CON EPES # CTC CON EPES, # ASFL DIURNAS # ASFL PERMANENTES # ESTANCIAS CONAPE 
    #'# CAIPI': '#  CAIPI',
    #'# Nuevos C. de Dia CONAPE': '#  Nuevos C. de Dia CONAPE',
    #'# CCPP CON EPES': '#  CCPP CON EPES',
    #'# CTC CON EPES': '#  CTC CON EPES',
    #'# ASFL DIURNAS': '#  ASFL DIURNAS',
    #'# ASFL PERMANENTES': '#  ASFL PERMANENTES',

    'capacidad total (CAIPI)': 'capacidad total (CAIPI)',
    'accesible_child_0to4_receiver_caipi': '# de 0 a 4 con a en trans',
    'perc_accesible_child_0to4_receiver_caipi': '% de 0 a 4 con a en trans',
    'capacidad total (CCPP CON EPES)': 'capacidad total (CCPP CON EPES)',
    'accesible_child_0to4_receiver_ccpp_epes': '# de 0 a 4 con a en trans (CCPP CON EPES)',
    'perc_accesible_child_0to4_receiver_ccpp_epes': '% de 0 a 4 con a en trans(CCPP CON EPES)',
    'capacidad total (CTC CON EPES)': 'capacidad total (CTC CON EPES)',
    'capacidad total (ASFL DIURNAS)': 'capacidad total (ASFL DIURNAS)',
    'accesible_elder_asfl_diurna': '# env dep con aceso en transporte (ASFL DIURNAS)',
    'perc_accesible_elder_asfl_diurna': '% env dep con aceso en transporte (ASFL DIURNAS)',
    'accesible_severe_asfl_diurna': '# env dep grave con a en trans (ASFL DIURNAS)',
    'perc_accesible_severe_asfl_diurna': '% env dep grave con a en trans (ASFL DIURNAS)',
    'capacidad total (ASFL PERMANENTES)': 'capacidad total (ASFL PERMANENTES)',
    'accesible_elder_asfl_permanente': '# env dep con aceso en transporte (ASFL PERMANENTES)',
    'perc_accesible_elder_asfl_permanente': '% env dep con aceso en transporte (ASFL PERMANENTES)',
    'accesible_severe_asfl_permanente': '# env dep grave con a en trans (ASFL PERMANENTES)',
    'perc_accesible_severe_asfl_permanente': '% env dep grave con a en trans (ASFL PERMANENTES)',
    #'# ESTANCIAS CONAPE': '#  ESTANCIAS CONAPE',
    'capacidad total (ESTANCIAS CONAPE)': 'capacidad total (ESTANCIAS CONAPE)',
    'accesible_elder_estancias_conape': '# env dep con aceso en transporte (ESTANCIAS CONAPE)',
    'perc_accesible_elder_estancias_conape': '% env dep con aceso en transporte (ESTANCIAS CONAPE)',
    'accesible_severe_estancias_conape': '# env dep grave con a en trans (ESTANCIAS CONAPE)',
    'perc_accesible_severe_estancias_conape': '% env dep grave con a en trans (ESTANCIAS CONAPE)',
    'capacidad total (Nuevos C. de Dia CONAPE)': 'capacidad total (Nuevos C. de Dia CONAPE)',
    'accesible_elder_nuevos_conape': '# env dep con aceso en transporte (Nuevos C. de Dia CONAPE)',
    'perc_accesible_elder_nuevos_conape': '% env dep con aceso en transporte (Nuevos C. de Dia CONAPE)',
    'accesible_severe_nuevos_conape': '# env dep grave con a en trans (Nuevos C. de Dia CONAPE)',
    'perc_accesible_severe_nuevos_conape': '% env dep grave con a en trans (Nuevos C. de Dia CONAPE)',
    'capacidad total (TOTAL CONAPE)': 'capacidad total (Centros CONAPE)',
    '# CCPP SIN EPES': '#  CCPP SIN EPES',
    
    '# COS INFOTEP': '#  COS INFOTEP',
    '# Direccion Regional INFOTEP': '# Direccion Regional INFOTEP', 
    '# Direccion Regional PROSOLI': '# Direccion Regional PROSOLI',
    '# CAID': '# CAID',
    '# Oficina de la Mujer': '# Oficina de la Mujer',
    
    'superate_coverage': 'Cobertura Superate',
    'coverage_caipi': 'Cobertura max capacidad CAIPI',
    'coverage_ccpp_epes': 'Cobertura max capacidad CCPP CON EPES',
    'coverage_ccpp_sin_epes': 'Cobertura max numero CCPP SIN EPES',
    'coverage_ctc_epes': 'Cobertura max capacidad  CTC CON EPES',
    'coverage_prosoli': 'Cobertura max numero Direccion Regional PROSOLI',
    'coverage_total_conape': 'Cobertura max capacidad CONAPE',
    'coverage_caid': 'Cobertura max numero CAID',
    'coverage_mujer': 'Cobertura max numero Oficina de la Mujer',
    'coverage_cos_infotep': 'Cobertura max numero centros COS INFOTEP',
    'coverage_infotep': 'Cobertura max numero Direccion Regional INFOTEP',
    
    
   #Indices
    'installed_cap_index': 'indice de cobertura institucional',
    'offer_index': 'indice de oferta institucional',
    #Numeradores demanda de cuidados
    'superate_disability_5to64_sum': '# de discapacitados (5 a 64)',
    'superate_child_0to4_receiver_sum': '# de 0 a 4 sin s c ni tanda escolar completa de cuidados',
    'superate_older_dependant_sum': '# de adultos mayores dependientes',
    #Denominador demanda de cuidado
    #'superate_care_giver':'# Total de cuidadores',
    #Numeradores presencia de cuidadoras
    'monomarental_with_superate_child': '# Casas monomarentales con niños',
    'monomarental_with_superate_disability': '# Casas monomarentales con discapacitados',
    'monomarental_with_superate_elder_dependant': '# Casas monomarentales con  envejecientes',
    'woman_domestic_worker_sum': '# de trabajadoras domésticas',
    #Denominador prescencia de cuidadores
    'household_id': '# hogares beneficiarios superate',
    #Ratios pobreza
    'percentage_poor_women':'porcentaje de mujeres pobres',
    'percentage_households_poor':'procentaje de hogares pobres',
    #Numeradores pobreza
    'woman_poor_sum' : '# Mujeres pobres',
    'is_poor': '# Casas pobres',
    #Denominadores
    'is_female_sum' : 'Total de mujeres', 
    }
    superate_households_col = 'household_id'    
    # prioritize_map_df = muni_cast_out.merge(
    #     influence_muni_stats.reset_index(), how='left').reset_index()
    # prioritize_map_df = prioritize_map_df.merge(infotep_muni_df, how='left').merge(ctc_muni_df)
    neigh_cast_out.loc[:, 'superate_coverage'] = neigh_cast_out.loc[ :, superate_households_col].div(neigh_cast_out[superate_households_col].sum())
    # prioritize_map_df.loc[:, 'prop_member_0to4_reachable'] = prioritize_map_df[
    #     'superate_child_0to4_receiver_reachable'].fillna(0).div(prioritize_map_df[
    #     'superate_child_0to4_receiver_sum'])
    if key_col =='neighborhood_key':
        prioritize_map_rename_dict = {
            **prioritize_map_rename_dict, 
            **{ 'neigh_muni': 'barrio'},
            }   

    #neigh_cast_out.set_index(neigh_key, inplace=True)
    prioritize_map_name_transformer = NameTransformer(
        {k: v.replace('ratio de ', '').replace('agregado', 'agregada').replace(
            '#', 'N').replace('porcentage', 'proporcion').replace(
            '%', 'proporcion') for k, v in prioritize_map_rename_dict.items()}, 
        keep_features=['geometry'])
    prioritize_map_export_neigh_gdf = prioritize_map_name_transformer.transform(neigh_cast_out)
    return prioritize_map_export_neigh_gdf


def format_varibles(df, n):
    ''' 
    Convert unreported values to 0. Unreported values happen when there are not SIUBEN households
    in agiven neighborhood.
    Reduce the number of decimals of the variable values to n points. 
    Converts to percentage scale the index variables
    '''
    
    percentaje_reporting_columns = [
     #'porcentage hogares 0 a 4',
     #'porcentage hogares (5 a 12)',
     #'porcentage hogares discapacitados 5 a 64',
     #'porcentage hogares discapacitado 13 a 64',
     #'porcentage hogares envejecientes',
     #'porcentage hogares envejecientes dependencia grave',
     #'porcentage hogares dependencia agregada',
     #'porcentage hogares dependencia agregada sin 5 a 12',
     #'porcentage hogares dependencia agregada sin env dep leves',
     #'porcentage hogares dependencia agregada sin 5 a 12 ni env dep leves',
     #'porcentage hogares con trabajadores domésticos',
     #'porcentage de mujeres trabajadoras domésticas',
     #'porcentage de hombres trabajadores domésticos',
     'idc-demanda de ciudado',
     'idc-presencia de ciudadoras',
     'idc-pobreza',
     'indice de priorizacion de demanda',
     #'% de 0 a 4 con a en trans',
     #'% de 0 a 4 con a en trans(CCPP CON EPES)',
     #'% env dep con aceso en transporte (ASFL DIURNAS)',
     #'% env dep grave con a en trans (ASFL DIURNAS)',
     #'% env dep con aceso en transporte (ASFL PERMANENTES)',
     #'% env dep grave con a en trans (ASFL PERMANENTES)',
     #'% env dep con aceso en transporte (ESTANCIAS CONAPE)',
     #'% env dep grave con a en trans (ESTANCIAS CONAPE)',
     #'% env dep con aceso en transporte (Nuevos C. de Dia CONAPE)',
     #'% env dep grave con a en trans (Nuevos C. de Dia CONAPE)',
     'Cobertura max capacidad CAIPI',
     'Cobertura max capacidad CCPP CON EPES',
     'Cobertura max numero CCPP SIN EPES',
     'Cobertura max capacidad  CTC CON EPES',
     'Cobertura max numero Direccion Regional PROSOLI',
     'Cobertura max capacidad CONAPE',
     'Cobertura max numero CAID',
     'Cobertura max numero Oficina de la Mujer',
     'Cobertura max numero centros COS INFOTEP',
     'Cobertura max numero Direccion Regional INFOTEP',
     'indice de cobertura institucional',
     'indice de oferta institucional',
    'porcentaje de mujeres pobres',
    'procentaje de hogares pobres']
    #TODO: Remove after testing
    percentaje_reporting_columns ={}
    
    
       
    for col in tqdm(percentaje_reporting_columns, 'format_varibles'):
        try: 
            #replace NA values, round to n decimals and cut 95%+ percentile tail
            df[col] =  df[col].replace(np.nan, 0 )
            df[col] = df[col].apply(lambda x: np.round(x, n))
            #We round observations after quantile 95 ans set it as maximum value
            # this helps improve visualization in carto. 
            #if np.sum(df[col] > df[col].quantile(.95)) < len(df):
            #We are trying to avoid to convert all observations to 
            df.loc[(df[col] > df[col].quantile(.95)), col] = df[col].quantile(.95)

        except Exception as e:
            print(e)
    for col in percentaje_reporting_columns:
        df[col] = df[col] * 100
    return df

if __name__ == "__main__":
    print('------------------- Runing exp0_export.py -------------------')