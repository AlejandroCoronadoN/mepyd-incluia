import logging, sys
from criteriaetl.transformers.columns_base import (NameTransformer, 
      ReplaceTransformer, SelectTransformer, AssignTransformer)
from criteriaetl.transformers.rows_base import (
      AggregateTransformer, DropTransformer)
from criteriaetl.transformers.fusion_base import MergeTransformer
from utils.u0_utils import non_repeated_columns
import geopandas as gpd
import numpy as np 
from shapely.geometry import Point

def mem_hous_merge_transformer(superate_at_out, sample_agg_miembros_df ):
   '''
   '''
   merge_transformer = MergeTransformer(lambda: superate_at_out, merge_kwargs=dict(
      on='household_id'))
   merged_df = merge_transformer.transform(sample_agg_miembros_df)
   
   return merged_df

def mem_hous_merge_select_transformer(merged_df):
   family_type_select_map = {
         # household level (select)
         'tipo_hogar_daes': {
               lambda df: (df['referente_sum'] == 1) & (
                     df['conyuge_sum'] == 0) & (df['hijo_sum'] == 0) & (
                     df['otro_pariente_sum'] == 0) & (df['no_pariente_sum'] == 0): 
                     "Unipersonal",
               lambda df: (df['referente_sum'] == 1) & (
                     df['conyuge_sum'] != 0) & (df['hijo_sum'] == 0) & (
                     df['otro_pariente_sum'] == 0) & (df['no_pariente_sum'] == 0): 
                     "Nuclear sin hijos",
               lambda df: (df['referente_sum'] == 1) & (
                     df['conyuge_sum'] != 0) & (df['hijo_sum'] != 0) & (
                     df['otro_pariente_sum'] == 0) & (df['no_pariente_sum'] == 0): 
                     "Nuclear con hijos",
               lambda df: (df['referente_sum'] == 1) & (
                     df['conyuge_sum'] == 0) & (df['hijo_sum'] != 0) & (
                     df['otro_pariente_sum'] == 0) & (df['no_pariente_sum'] == 0): 
                     "Nuclear monoparental",
               lambda df: (
                     df['referente_sum'] == 1) & (df['conyuge_sum'] != 0) & (
                     df['hijo_sum'] == 0) & (df['otro_pariente_sum'] != 0)
               & (df['no_pariente_sum'] == 0): "Extendido base nuclear sin hijos",
               lambda df: (
                     df['referente_sum'] == 1) & (df['conyuge_sum'] != 0) & (
                     df['hijo_sum'] != 0) & (df['otro_pariente_sum'] != 0) & (
                     df['no_pariente_sum'] == 0): "Extendido base nuclear con hijos",
               lambda df: (df['referente_sum'] == 1) & (
                     df['conyuge_sum'] == 0) & (df['hijo_sum'] != 0) & (
                     df['otro_pariente_sum'] != 0) & (df['no_pariente_sum'] == 0): 
                     "Extendido base monoparental",
               lambda df: (df['referente_sum'] == 1) & (
                     df['conyuge_sum'] == 0) & (df['hijo_sum'] == 0) & (
                     df['otro_pariente_sum'] != 0) & (df['no_pariente_sum'] == 0): 
                     "Extendido sin base nuclear",
               lambda df: (df['referente_sum'] == 1) & (
                     df['no_pariente_sum'] != 0): "Compuesto",
         }
   }

   family_select_transformer = SelectTransformer(family_type_select_map)
   fst_out = family_select_transformer.transform(merged_df)
   return fst_out
   
def mem_hous_merge_assign_trasnformer(fst_out):
   dependance_col_dict = {
         'dependence_0to4_rate': 'superate_child_0to4_receiver',
         'dependence_5to12_rate': 'superate_child_5to12_receiver',
         'dependence_dissability_13to64_rate': 'superate_disability_13to64',
         'dependence_dissability_5to64_rate': 'superate_disability_5to64',
         'dependence_older_dependant_rate': 'superate_older_dependant',
         'dependence_older_severe_dependant_rate': 
               'superate_older_severe_dependant',
         'dependence_rate_micro': 'superate_care_receiver', #<- Validacion
         'dependence_rate_micro_wo_mild_dependant': 
               'superate_care_receiver_wo_mild_dependant',
         'dependence_rate_micro_wo_5to12': 'superate_care_receiver_wo_5to12',
         'dependence_rate_micro_wo_both': 'superate_care_receiver_wo_both',
   }
   dependance_assign_map = {
   dependance_col: lambda df, numerator=care_receiver_col : np.where(
               df['superate_care_giver_sum'].gt(0), 
               df[f'{numerator}_sum'].div(df['superate_care_giver_sum']),
               df[f'{numerator}_sum']) for (
                     dependance_col, care_receiver_col) in dependance_col_dict.items()
   }
   household_assign_map = {
         **dependance_assign_map,
         # monomarental families
         'monomarental': lambda df: df['tipo_hogar_daes'].isin([
               'Nuclear monoparental', 'Extendido base monoparental']) & df[
               'woman_led_any'],
         'monomarental_with_superate_child': lambda df: df['monomarental'] & df[
               'member_0to4_any'],
         'monomarental_with_superate_disability': lambda df: df['monomarental'] & 
               df['superate_disability_5to64_any'],
         'monomarental_with_superate_elder_dependant': lambda df: df[
               'monomarental'] & df['superate_older_dependant_any'],
         # poverty
         'is_poor': lambda df: df['icv_cat'].isin([1, 2]),
         'woman_poor_sum': lambda df: df['is_female_sum'] * df['is_poor'],
   }
   household_assign_transformer = AssignTransformer(household_assign_map)
   household_ast_out = household_assign_transformer.transform(fst_out)
   return household_ast_out

def mem_hous_merge_drop_transformer(household_ast_out):
   imputed_beneficiaries_col = 'superate_imputed'
   superate_drop_dict = {imputed_beneficiaries_col: [False] }
   superate_drop_transformer = DropTransformer(superate_drop_dict)
   superate_dt_out = superate_drop_transformer.transform(household_ast_out)
   return superate_dt_out
   
def mem_hous_merge_name_transformer(superate_dt_out):
   area_stats_keep = [
         'lat', 'lon', 'municipality_key', 'neighborhood_key',
         'member_0to4_sum', 'recibe_ciudados_inaipi_sum', 
         'recibe_ciudados_publico_no_inaipi_sum', 'recibe_ciudados_privado_sum', 
         'recibe_ciudados_otros_sum','superate_child_0to4_receiver_sum', 
         'member_5to12_sum', 'superate_child_5to12_receiver_sum', 
         'superate_disability_13to64_sum', 'superate_disability_5to64_sum',
         'superate_older_dependant_sum', 'superate_older_severe_dependant_sum', 
         'woman_domestic_worker_sum', 'woman_work_age_sum', 
         'man_domestic_worker_sum', 'man_work_age_sum','member_id_count', 
         'superate_child_0to4_receiver_any', 
         'superate_child_5to12_receiver_any', 
         'superate_disability_13to64_any', 'superate_disability_5to64_any',
         'superate_older_dependant_any', 'superate_older_severe_dependant_any',
         'superate_care_receiver_any', 
         'superate_care_receiver_wo_mild_dependant_any', 
         'superate_care_receiver_wo_5to12_any', 
         'superate_care_receiver_wo_both_any', 'domestic_worker_any', 
         'household_id',
         'dependence_0to4_rate', 'dependence_5to12_rate', 
         'dependence_dissability_13to64_rate', 'dependence_dissability_5to64_rate', 
         'dependence_older_dependant_rate', 
         'dependence_older_severe_dependant_rate',
         'dependence_rate_micro_wo_both', 'dependence_rate_micro_wo_5to12',
         'dependence_rate_micro_wo_mild_dependant', 'dependence_rate_micro', 
         # prioritization index
         'woman_houseworker_sum', 'monomarental_with_superate_child', 
         'monomarental_with_superate_disability', 
         'monomarental_with_superate_elder_dependant',
         'woman_poor_sum', 'is_poor', 'is_female_sum'
   ]
   geo_superate_name_transformer = NameTransformer({}, area_stats_keep)
   src_superate_gdf = geo_superate_name_transformer.transform(superate_dt_out)
   return src_superate_gdf


def to_geodata(src_superate_gdf, latitude_col: str = 'lat', longitude_col:str = 'lon', goemetry_col:str = 'geometry'):
   src_superate_gdf.loc[:, goemetry_col] = src_superate_gdf[[
         longitude_col, latitude_col]].apply( lambda *args: Point(tuple(*args)), axis=1)

   superate_gdf = gpd.GeoDataFrame( src_superate_gdf, crs="EPSG:4326", geometry='geometry')
   return superate_gdf


def merge_mem_hous_areas(superate_gdf, areas_gdf):
    nr_cols  = non_repeated_columns(superate_gdf, areas_gdf)
    merged_superate_gdf = gpd.sjoin(superate_gdf, areas_gdf[nr_cols + ['geometry']], op='within')
    return merged_superate_gdf




def mem_hous_area_rename_transformer(cast_out):
    #TODO: dissolved_influence_areas might not be require, integrate or create a new function
    dissolved_influence_areas = False
    area_map_rename_dict = {
    'id': 'id area',
    'nombre': 'nombre de centro',
    'tipo': 'tipo de centro',
    'poblacion_objetivo': 'poblacion objetivo',
    'municipio': 'municipio',
    'neighborhood_key':'neighborhood_key',
    'barrio':'barrio',
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
    'dependence_rate_micro_wo_mild_dependant': 'ratio parcial de dependencia '
    'agregada sin env dep leves', 
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
    'superate_child_0to4_receiver_sum':  '# de 0 a 4 sin s c ni tanda escolar completa de cuidados', 
    'member_5to12_sum': '# de personas de 5 a 12', 
    'superate_child_5to12_receiver_sum': '# de 5 a 12 sin s c ni tanda escolar completa',
    'superate_disability_5to64_sum': '# de discapacitados (5 a 64)',
    'superate_disability_13to64_sum': '# de discapacitados (13 a 64)',
    'superate_older_dependant_sum': '# de adultos mayores dependientes',
    'superate_older_severe_dependant_sum': '# de adultos mayores dependientes graves',
    'member_id_count': '# personas superate',
    'superate_child_0to4_receiver_any': '# hogares con miembros de 0 a 4 sin s c ni tanda escolar completa', 
    'superate_child_5to12_receiver_any': '# hogares con miembros de 5 a 12 sin s c ni tanda escolar completa', 
    'superate_disability_5to64_any': '# hogares con miembros discapacitados (5 a 64)',
    'superate_disability_13to64_any': '# hogares con miembros discapacitados (13 a 64)', 
    'superate_older_dependant_any': '# hogares con miembros adultos mayores dependientes', 
    'superate_older_severe_dependant_any': '# hogares con miembros adultos mayores dependientes graves',
    'household_id': '# hogares beneficiarios superate',
    
    'capacidad': 'capacidad de centro',
    'superate_coverage': 'porcentage hogares superate',
    
    'domestic_worker_any': '# de hogares con trabajadores domésticos',
    'percentage_households_domestic_worker': 'porcentage hogares con trabajadores domésticos',
    'woman_domestic_worker_sum': '# de trabajadoras domésticas',
    'woman_work_age_sum': '# de mujeres en edad de trabajar',
    'percentage_woman_domestic_worker': 'porcentage de mujeres trabajadoras domésticas',
    'man_domestic_worker_sum': '# de trabajadores domésticos',
    'man_work_age_sum': '# de hombres en edad de trabajar',
    'percentage_men_domestic_worker': 'porcentage de hombres trabajadores domésticos',
    # 'member_0to4_reachable': '# de personas con acceso en transporte de 0 a 4 ',
    # 'prop_member_0to4_reachable': 'personas de 0 a 4 con acceso en transporte',
    # 'dependence_0to4_reachable': 'tasa de dependencia (con acceso en transporte de 0 a 4)',
    'demanda_de_ciudado': 'idc-demanda de ciudado', 
    'presencia_de_ciudadoras': 'idc-presencia de ciudadoras', 
    'pobreza': 'idc-pobreza', 
    'indice_priorizacion_ciudados': 'indice de priorizacion de demanda',
    'indice_agregado_demanda': 'indice de demanda',

    }
    area_map_rename_dict.update({
        'caipi_sum': '# de CAIPI'}) if dissolved_influence_areas else None
    area_map_name_transformer = NameTransformer(
        {k: v.replace('ratio de ', '').replace(
            '#', 'N').replace('porcentage', 'proporcion') for k, v in 
        area_map_rename_dict.items()}, 
        keep_features=['geometry'])
    area_map_export_gdf = area_map_name_transformer.transform(cast_out)
    return cast_out