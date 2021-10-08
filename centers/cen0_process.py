import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
from difflib import get_close_matches
from shapely.geometry import Point

# get timestamp in utc format
from datetime import datetime
timestamp = datetime.utcnow().strftime('%Y%m%dT%H%MZ')

from criteriaetl.utils.display_func import (cdisplay, rdisplay, 
    percentage_count_plot)
from criteriaetl.transformers.fusion_base import MergeTransformer
from criteriaetl.transformers.columns_base import (NameTransformer, 
    ReplaceTransformer, SelectTransformer, AssignTransformer)
from projectetl.utils.config import data_dir
from projectetl.utils.dataload import load_s3_data_do
from projectetl.utils.save import protected_save
from projectetl.utils.process import name_normalizer
from projectetl.utils.dataload import get_path_from_pattern
from projectetl.utils.display import plot_interactive_geojson
from utils.u0_utils import non_repeated_columns
import geopandas  as gpd
import pandas as pd 
from utils.u0_utils import dissolve_dataframe_areas, non_repeated_columns

def influenceareas_process(centers_areas_walk, centers_areas_car, gdf_areas):
    '''
    '''
    gdf_areas['id'] = pd.to_numeric(gdf_areas['id'])
    centers_areas_walk = gpd.GeoDataFrame(centers_areas_walk, crs="EPSG:4326", geometry='geometry')
    centers_areas_walk = centers_areas_walk[['id','geometry']]

    centers_areas_car = gpd.GeoDataFrame(centers_areas_car, crs="EPSG:4326", geometry='geometry')
    centers_areas_car =centers_areas_car[['id','geometry']]
    #Dissolve influence areas
    centers_areas_car_walk = dissolve_dataframe_areas(centers_areas_walk, centers_areas_car)
    nr_cols = non_repeated_columns(centers_areas_car_walk, gdf_areas)
    nr_cols.append('id')
    gdf_areas_processed = centers_areas_car_walk.merge( gdf_areas[nr_cols], on='id', how ='inner')
    return gdf_areas_processed, centers_areas_car_walk

def process_centers(gdf_municipality_processed, gdf_neighborhood_processed):
    '''DATA SOURCES:
    gdf_provinces = gpd.read_file(f'data/raw/provinces_raw.geojson', encoding = 'utf-8')# [cols_provinces_gpd].copy()
    df_caid_raw = pd.read_excel( 'data/raw/caid_raw.xlsx')
    gdf_minmujer = gpd.read_file('data/raw/minmujer_raw.geojson', encoding = 'utf-8')
    gdf_othcercenters = gpd.read_file('data/raw/othercenters_raw.geojson', encoding = 'utf-8').dropna(subset=['latitude'])
    gdf_caipiarea = gpd.read_file('data/raw/caipi_influencearea_raw.geojson', encoding = 'utf-8')
    gdf_caipiarea_inconsistent = gpd.read_file('data/raw/caipi_inconsistent_raw.geojson', encoding = 'utf-8')
    gdf_caidminmujer_area = gpd.read_file('data/raw/caidminmujer_area_raw.geojson', encoding = 'utf-8')
    gdf_otherareas2 = gpd.read_file('data/raw/otherareas1_raw.geojson', encoding = 'utf-8').dropna(subset=['latitude'])
    df_ctc = pd.read_excel(f'data/raw/ctc_raw.xlsx', skiprows=1, header=[0, 1]).dropna(how='all', axis=1).drop_duplicates()
    df_infotep = pd.read_excel(f'data/raw/infotep_raw.xlsx', skiprows=4).dropna(how='all', axis=1).fillna(method='ffill')

    '''
    runall = True
    dissolve_influence_areas = False # only implemented for CAIPIS 

    # ## Center Locations

    gdf_provinces = gpd.read_file(f'data/raw/provinces_raw.geojson') # [cols_provinces_gpd].copy()

    # ## Center Locations

    # ### Points
    # #### CAIPI
    caipi = gpd.read_file(f'data/raw/caipi_raw.geojson').dropna(subset=['Latitud'])


    # #### CAID
    df_caid_raw = pd.read_excel( 'data/raw/caid_raw.xlsx')

    # transform to geo data frame
    caid_gdf = df_caid_raw.copy()
    caid_gdf.loc[:, 'geometry'] = caid_gdf[[
        'Long', 'Lat']].apply(
            lambda *args: Point(tuple(*args)), axis=1)

    caid_gdf = gpd.GeoDataFrame(
        caid_gdf, crs="EPSG:4326", geometry='geometry')


    # #### MIN Mujer
    gdf_minmujer = gpd.read_file('data/raw/minmujer_raw.geojson')


    # #### Other Centers
    gdf_othcercenters = gpd.read_file('data/raw/othercenters_raw.geojson').dropna(subset=['latitude'])

    # Prosoli regional bureau  Norcentral II does not exist, according to Jorge 
    # message on June 21.
    gdf_othcercenters.drop(
        gdf_othcercenters.index[
            gdf_othcercenters.level_0.eq('prosoli') & gdf_othcercenters.itm.eq(
                9)], inplace=True)


    # ### Influence Areas
    gdf_caipiarea = gpd.read_file('data/raw/caipi_influencearea_raw.geojson')
    # before correction
    # gdf_caipiarea[gdf_caipiarea.numcentro.isin([29, 32, 36])].plot()
    for idx, geometry in gpd.read_file('data/raw/caipi_inconsistent_raw.geojson').set_index('n_centro')['geometry'].iteritems():
        gdf_caipiarea.loc[
            gdf_caipiarea.numcentro.eq(idx), 'geometry'] = geometry
    gdf_caipiarea.columns

    gdf_caidminmujer_area = gpd.read_file('data/raw/caidminmujer_area_raw.geojson')

    gdf_otherareas1 = gpd.read_file('data/raw/otherareas1_raw.geojson').dropna(subset=['latitude'])

    # drop care center category as agreed 
    gdf_otherareas1 = gdf_otherareas1[lambda df: df.level_0.ne('HARAS NACIONALES')]
    # Prosoli regional bureau  Norcentral II does not exist, according to Jorge 
    # message on June 21.
    gdf_otherareas1.drop(
        gdf_otherareas1.index[
            gdf_otherareas1.level_0.eq('prosoli') & \
                gdf_otherareas1.itm.eq(9)], inplace=True)

    # join columns which were not added in this geolocation process
    gdf_otherareas1 = gdf_otherareas1.merge(gdf_othcercenters[[
        'level_0', 'itm', 'municipality_key', 'direccion', 'nombre']], on=[
        'level_0', 'itm'])

    gdf_otherareas2 = gpd.read_file('data/raw/otherareas1_raw.geojson').dropna(subset=['latitude'])

    other_located_areas = pd.concat((
        gdf_otherareas1, gdf_otherareas2), ignore_index=True)


    # ### Non geolocated care and training centers

    # In[16]:


    df_ctc = pd.read_excel(f'data/raw/ctc_raw.xlsx', skiprows=1, header=[0, 1]).dropna(how='all', axis=1).drop_duplicates()
    df_ctc = df_ctc.dropna()
    ctc_cols = [
        'NOMBRE DEL CTC', 'PROVINCIA', 'Municipio', 'CAPACIDAD FISICA'
    ]
    df_ctc.columns = [s.lower() for s in ctc_cols]


    # ## Centers with boundaries
    # 
    # - CTC's are already covered as their boundaries coincide with municipality boundaries.
    # - INFOTEP and PROSOLI regional bureaux are loaded.

    # ### INFOTEP

    # In[17]:


    df_infotep = pd.read_excel(f'data/raw/infotep_raw.xlsx', skiprows=4).dropna(how='all', axis=1).fillna(method='ffill')
    df_infotep.columns = df_infotep.columns.str.lower()


    # ### PROSOLI
    df_prosoli = pd.read_excel(f'data/raw/prosoli_raw.xlsx',skiprows=4).dropna(subset=['parsed_provincia'])
    df_prosoli.columns = name_normalizer(df_prosoli.columns)

    # ## Center Capacities
    # ### CAIPI capacities
    caipi_capacity_panel = pd.read_pickle(f'data/raw/caipi_capacity_raw.pk')
    caipi_capacity_dict = caipi_capacity_panel['2021-02-15'].to_dict()

    caipi_capacity_dict
    caipi_areas_gdf = gdf_caipiarea.assign(
        influence_area=lambda df: df['numcentro'], 
        influence_area_name=lambda df: df['centro'])



    normalized_muni_col = 'norm_municipio'
    caipi_pre_concat_assign_map = {
        # municipality key to caipi
        normalized_muni_col: lambda df: name_normalizer(df.municipio).str.replace(
            'bisono (navarrete)', 'bisono', regex=False), 
        'municipality_key' : lambda df: df.merge(gdf_municipality_processed.assign(**{
            normalized_muni_col: lambda df: name_normalizer(df.Municipio)})[[
            normalized_muni_col, 'municipality_key'
        ]], how='left')['municipality_key'].values,

        'municipality_key' : lambda df: df.merge(gdf_municipality_processed.assign(**{
            normalized_muni_col: lambda df: name_normalizer(df.Municipio)})[[
            normalized_muni_col, 'municipality_key'
        ]], how='left')['municipality_key'].values,
        
        'capacity': lambda df: df['municipality_key'].map(
            caipi_capacity_dict).round(0).astype(int)
    }

    caipi_pre_concat_assign_transformer = AssignTransformer(caipi_pre_concat_assign_map)

    caipi_merged_gdf = caipi.copy()
    caipi_merged_gdf.columns = caipi_merged_gdf.columns.str.lower()
    caipi_merged_gdf = caipi_pre_concat_assign_transformer.transform(caipi_merged_gdf)


    #MERGE
    caipi_barrios_gdf = gpd.sjoin(caipi_merged_gdf[['region', 'provincia', 'municipio', 'numcentro', 'centro', 'tipo',
        'direccion', 'latitud', 'longitud', 'geometry', 'norm_municipio','capacity']],  gdf_neighborhood_processed,  op='within')

    # ### CTC
    ctc_muni_src_df = df_ctc.copy()
    ctc_muni_src_df = ctc_muni_src_df.assign( tipo='CTC con EPES', direccion=np.nan, itm=lambda df: range(1, df.shape[0] + 1))

    #ctc_muni_src_df['Municipio'] = df_ctc.municipio.map(lambda x: get_close_matches(x, gdf_municipality_processed.Municipio.values, n=1)[0])
    
    ctc_muni_src_df['Municipio'] = df_ctc.municipio
    for mun in df_ctc.municipio.unique():
        close_mun = get_close_matches(mun, gdf_municipality_processed.Municipio.values, n=1)[0]
        ctc_muni_src_df.loc[(ctc_muni_src_df.municipio ==mun) ,'Municipio'] = close_mun

    ctc_muni_src_gdf = gdf_municipality_processed[[
        'Municipio', 'municipality_key', 'geometry']].merge(
        ctc_muni_src_df)
    # for care centers geojson
    ctc_muni_centroids_gdf = ctc_muni_src_gdf.copy()
    ctc_muni_centroids_gdf['geometry'] = ctc_muni_centroids_gdf[
        'geometry'].centroid.to_crs(crs="EPSG:4326")


    # ### CTC - Barrios
    duplicated_columns = ['municipality_key', 'index']

    ctc_neigh_centroids_gdf = ctc_muni_centroids_gdf.copy()
    non_duplicated_columns = [x for x in ctc_neigh_centroids_gdf.columns if x not in duplicated_columns]

    ctc_neigh_centroids_gdf = gpd.sjoin(ctc_neigh_centroids_gdf[non_duplicated_columns], gdf_neighborhood_processed, op='within')

    # ### CAID
    caid_to_merge_gdf = caid_gdf.assign(**{
        'name': lambda df: 'CAID ' + df['Municipio'],
        'capacity': np.nan,
        'tipo': 'CAID',
        'itm': lambda df: df.index,
        'municipality_key': lambda df: df['municipio'].str.extract('(\d{4})')
    })


    # ### CAID - Barrios
    caid_to_merge_gdf = caid_gdf.assign(**{
        'name': lambda df: 'CAID ' + df['Municipio'],
        'capacity': np.nan,
        'tipo': 'CAID',
        'itm': lambda df: df.index,
        'municipality_key': lambda df: df['municipio'].str.extract('(\d{4})')
    })

    nonrepeated_cols = non_repeated_columns(caid_to_merge_gdf, gdf_neighborhood_processed, [], [], True)
    caid_neigh_gdf = gpd.sjoin(caid_to_merge_gdf, gdf_neighborhood_processed[nonrepeated_cols])


    # ### Ministerio de la Mujer
    min_mujer_to_merge_gdf = gdf_minmujer.assign(**{
        'name': lambda df: 'Oficina de la Mujer ' + df['Municipio'],
        'capacity': np.nan,
        'tipo': 'Oficina de la Mujer',
        'direccion': np.nan,
        'itm': lambda df: df.index,
    })
    min_mujer_to_merge_gdf.head()


    # ### Ministerio de la Mujer -  Neighborhoods
    nonrepeated_cols = non_repeated_columns(min_mujer_to_merge_gdf, gdf_neighborhood_processed, [], [], True)
    min_mujer_neigh_gdf = gpd.sjoin(min_mujer_to_merge_gdf, gdf_neighborhood_processed[nonrepeated_cols])


    # ### Other geolocated centers
    other_centers_pre_concat_select_map = {
        'center_type': {
            lambda df: df.level_0.eq('ccpp') & (~ df.has_epes): 
                'CCPP SIN EPES',
            lambda df: df.level_0.eq('ccpp') & df.has_epes: 
                'CCPP CON EPES',
            'default': lambda df: df['level_0']
        }
    }

    other_centers_pre_concat_select_transformer = SelectTransformer(
        other_centers_pre_concat_select_map)
    other_centers_merged_gdf = gdf_othcercenters.pipe(
    # .fillna({
    #     'has_epes': False, 'capacidad': np.nan}).astype({'has_epes': bool})
    other_centers_pre_concat_select_transformer.transform)

    # ### Other geolocated centers - Barrios
    nonrepeated_cols = non_repeated_columns(other_centers_merged_gdf, gdf_neighborhood_processed, [], [], True)
    other_centers_neigh_gdf = gpd.sjoin(other_centers_merged_gdf, gdf_neighborhood_processed[nonrepeated_cols], op='within').drop('index_right', axis=1)

    # ## Concatenate -  Municipio
    concat_map = {
        'name': ['centro', 'nombre del ctc', 'name', 'name', 'nombre'],
        'capacity': [
            'capacity', 'capacidad fisica', 'capacity', 'capacity', 'capacidad'], 
        'tipo': ['tipo', 'tipo', 'tipo', 'tipo', 'center_type'], 
        'direccion': [
            'direccion', 'direccion', 'Dirección', 'direccion', 'direccion'],
        'itm': ['numcentro', 'itm',  'itm', 'itm', 'itm']
    }
    keep_features = ['municipality_key', 'geometry']


    def rename_to_concat(df, i, concat_map=concat_map, keep_features=keep_features):
        return NameTransformer({
            v[i]: k for k, v in concat_map.items()}, keep_features=keep_features
        ).transform(df) 

    concat_caipi = rename_to_concat(caipi_merged_gdf, 0)
    concat_ctc = rename_to_concat(ctc_muni_centroids_gdf, 1)
    concat_caid = rename_to_concat(caid_to_merge_gdf, 2)
    concat_min_mujer = rename_to_concat(min_mujer_to_merge_gdf, 3)
    concat_other = rename_to_concat(other_centers_merged_gdf, 4)
    concat_centers = pd.concat((
        concat_caipi, concat_ctc, concat_caid, concat_min_mujer, concat_other), 
        ignore_index=True)


    # In[ ]:





    # ## Concatenate - Barrio

    # In[35]:


    caipi_barrios_gdf.columns


    # In[36]:


    # target_column: [CAIPI, ctc, caid, min_mujer, other]
    concat_map = {
        'name': ['centro', 'nombre del ctc', 'name', 'name', 'nombre'],
        'capacity': [
            'capacity', 'capacidad fisica', 'capacity', 'capacity', 'capacidad'], 
        'tipo': ['tipo', 'tipo', 'tipo', 'tipo', 'center_type'], 
        'direccion': [
            'direccion', 'direccion', 'Dirección', 'direccion', 'direccion'],
        'itm': ['numcentro', 'itm',  'itm', 'itm', 'itm']
    }
    keep_features = ['municipality_key', 'neighborhood_key', 'geometry']


    def rename_to_concat(df, i, concat_map=concat_map, keep_features=keep_features):
        return NameTransformer({
            v[i]: k for k, v in concat_map.items()}, keep_features=keep_features
        ).transform(df) 

    concat_caipi = rename_to_concat(caipi_barrios_gdf, 0)
    concat_ctc = rename_to_concat(ctc_neigh_centroids_gdf, 1)
    concat_caid = rename_to_concat(caid_neigh_gdf, 2)
    concat_min_mujer = rename_to_concat(min_mujer_neigh_gdf, 3)
    concat_other = rename_to_concat(other_centers_neigh_gdf, 4)

    concat_neig_centers = pd.concat((
        concat_caipi, concat_ctc, concat_caid, concat_min_mujer, concat_other), 
        ignore_index=True)

    caipi_barrios_gdf.columns

    query_areas_gdf = pd.concat((
        concat_caid, concat_min_mujer), 
        ignore_index=True)

    # ## Export - Municipio

    # In[38]:


    assert concat_centers[['tipo', 'itm']].duplicated().sum() == 0, (
        f"There are duplicates in ID columns")


    # In[39]:


    center_types = concat_centers.tipo.unique()
    center_types_dict = dict(zip(sorted(center_types), range(1, len(
        center_types) + 1)))


    # In[40]:


    center_types_dict


    # In[41]:


    merge_municipalities = MergeTransformer(lambda: gdf_municipality_processed[[
        'municipality_key', 'Municipio', 'Provincia']])
    export_select_map = {
        'objective_pop': {
            lambda df: df.tipo.isin(['ASFL PERMANENTES ONGS', 'ASFL DIURNAS',
            'ESTANCIAS QUE SON DE  CONAPE', 'NUEVOS CENTRO DE DIA CONAPE']): 
                'adulto mayor dependiente',
            lambda df: df.tipo.isin([
                'COS-Central', 'COS-Este', 'COS-Norte', 'COS-Sur', 'CCPP SIN EPES'
            ]): 'centro de formacion',
            lambda df: df.tipo.isin([
                'prosoli', 'INFOTEP', 'Oficina de la Mujer'
            ]): 'direccion regional',
            lambda df: df.tipo.isin(['CAIPI', 'CTC con EPES', 'CCPP CON EPES']): 
                'primera infancia',
            lambda df: df.tipo.eq('CAID'): 'situacion de discapacidad',
            'default': np.nan        
        },
    }
    export_assign_map = {
        # 'cap': lambda df: df['capacity'].astype('Int64'),
        'capacidad': lambda df: df['capacity'].fillna(df['objective_pop'].map(
            df.groupby('objective_pop')['capacity'].median().to_dict())),
        'id': lambda df: df['tipo'].map(center_types_dict).astype(str).str.zfill(
            2).str.cat(df.itm.astype(str).str.zfill(4)),
        'municipio': lambda df: df['municipality_key'].str.cat(
            df['Municipio'].str.cat(df['Provincia'], sep=', '),
            sep=' - '),
        'nombre': lambda df: name_normalizer(df['name']).str.title(),
        'leyenda': lambda df: df['tipo']
    }

    tipo_replace_map = {
        # Primera infancia
        **{c: f'Primera Infancia - {c.upper()}' for c in [
            'CAIPI', 'CCPP CON EPES', 'CTC con EPES']},
        # Adultos Mayores
        'ASFL DIURNAS': 'Adultos Mayores - ASFL DIURNAS',
        'ASFL PERMANENTES ONGS': 'Adultos Mayores - ASFL PERMANENTES',
        'ESTANCIAS QUE SON DE  CONAPE': 'Adultos Mayores - ESTANCIAS CONAPE',
        'NUEVOS CENTRO DE DIA CONAPE': 'Adultos Mayores - Nuevos C. de Dia CONAPE',
        # Discapacitados
        'CAID': 'Situacion de Discapacidad - CAID',
        # Centros de Formacion
        'CCPP SIN EPES': 'C. de Formacion - CCPP SIN EPES',
        **{c: 'C. de Formacion - COS INFOTEP' for c in [
            'COS-Central', 'COS-Este', 'COS-Norte', 'COS-Sur']},
        # Direcciones Regionales
        'prosoli': 'Direccion Regional PROSOLI',
        'INFOTEP': 'Direccion Regional INFOTEP',
    }

    export_replace_map = {
        'tipo': tipo_replace_map,
        'leyenda': {
            **tipo_replace_map,
            # Adultos Mayores
            **{type_: 'Adultos Mayores - Centros CONAPE' for type_ in [
                'ASFL DIURNAS', 'ASFL PERMANENTES ONGS', 
                'ESTANCIAS QUE SON DE  CONAPE', 'NUEVOS CENTRO DE DIA CONAPE'
            ]}
        }
    }

    export_name_map = {
        'objective_pop': 'poblacion_objetivo', 
    }
    export_name_keep = [
        'leyenda', 'nombre', 'id', 'capacidad', 'direccion', 'tipo', 'municipio', 'geometry'
    ]
    export_centers = concat_centers    .pipe(merge_municipalities.transform)    .pipe(SelectTransformer(export_select_map).transform)    .pipe(AssignTransformer(export_assign_map).transform)    .pipe(ReplaceTransformer(export_replace_map, method='replace').transform)    .pipe(NameTransformer(export_name_map, export_name_keep).transform)

    concat_centers.pipe(merge_municipalities.transform).columns


    merge_neighborhoods = MergeTransformer(lambda: gdf_neighborhood_processed[[
        'municipality_key', 'neighborhood_key', 'Municipio', 'Provincia', 'TOPONIMIA']])
    export_select_map_neigh = {
        'objective_pop': {
            lambda df: df.tipo.isin(['ASFL PERMANENTES ONGS', 'ASFL DIURNAS',
            'ESTANCIAS QUE SON DE  CONAPE', 'NUEVOS CENTRO DE DIA CONAPE']): 
                'adulto mayor dependiente',
            lambda df: df.tipo.isin([
                'COS-Central', 'COS-Este', 'COS-Norte', 'COS-Sur', 'CCPP SIN EPES'
            ]): 'centro de formacion',
            lambda df: df.tipo.isin([
                'prosoli', 'INFOTEP', 'Oficina de la Mujer'
            ]): 'direccion regional',
            lambda df: df.tipo.isin(['CAIPI', 'CTC con EPES', 'CCPP CON EPES']): 
                'primera infancia',
            lambda df: df.tipo.eq('CAID'): 'situacion de discapacidad',
            'default': np.nan        
        },
    }
    export_assign_map_neigh= {
        # 'cap': lambda df: df['capacity'].astype('Int64'),
        'capacidad': lambda df: df['capacity'].fillna(df['objective_pop'].map(
            df.groupby('objective_pop')['capacity'].median().to_dict())),
        'id': lambda df: df['tipo'].map(center_types_dict).astype(str).str.zfill(
            2).str.cat(df.itm.astype(str).str.zfill(4)),
        'municipio': lambda df: df['municipality_key'].str.cat(
            df['Municipio'].str.cat(df['Provincia'], sep=', '),
            
            sep=' - '),
        
        'barrio': lambda df: df['neighborhood_key'].str.cat(
                df['TOPONIMIA'].str.cat( df['Municipio'].str.cat(df['Provincia'], sep=', ') , sep=', '), sep=' - '),

        'nombre': lambda df: name_normalizer(df['name']).str.title(),
        'leyenda': lambda df: df['tipo']
    }

    tipo_replace_map = {
        # Primera infancia
        **{c: f'Primera Infancia - {c.upper()}' for c in [
            'CAIPI', 'CCPP CON EPES', 'CTC con EPES']},
        # Adultos Mayores
        'ASFL DIURNAS': 'Adultos Mayores - ASFL DIURNAS',
        'ASFL PERMANENTES ONGS': 'Adultos Mayores - ASFL PERMANENTES',
        'ESTANCIAS QUE SON DE  CONAPE': 'Adultos Mayores - ESTANCIAS CONAPE',
        'NUEVOS CENTRO DE DIA CONAPE': 'Adultos Mayores - Nuevos C. de Dia CONAPE',
        # Discapacitados
        'CAID': 'Situacion de Discapacidad - CAID',
        # Centros de Formacion
        'CCPP SIN EPES': 'C. de Formacion - CCPP SIN EPES',
        **{c: 'C. de Formacion - COS INFOTEP' for c in [
            'COS-Central', 'COS-Este', 'COS-Norte', 'COS-Sur']},
        # Direcciones Regionales
        'prosoli': 'Direccion Regional PROSOLI',
        'INFOTEP': 'Direccion Regional INFOTEP',
    }

    export_replace_map_neigh = {
        'tipo': tipo_replace_map,
        'leyenda': {
            **tipo_replace_map,
            # Adultos Mayores
            **{type_: 'Adultos Mayores - Centros CONAPE' for type_ in [
                'ASFL DIURNAS', 'ASFL PERMANENTES ONGS', 
                'ESTANCIAS QUE SON DE  CONAPE', 'NUEVOS CENTRO DE DIA CONAPE'
            ]}
        }
    }
    export_name_map = {
        'objective_pop': 'poblacion_objetivo', 
    }
    export_name_keep = [
        'leyenda', 'nombre', 'id', 'capacidad', 'direccion', 'tipo', 'municipio', 'geometry', 'TOPONIMIA', 'neighborhood_key', 'barrio'
    ]
    gdf_centers_processed = concat_neig_centers.pipe(merge_neighborhoods.transform)    .pipe(SelectTransformer(export_select_map_neigh).transform)    .pipe(AssignTransformer(export_assign_map_neigh).transform)    .pipe(ReplaceTransformer(export_replace_map_neigh, method='replace').transform)    .pipe(NameTransformer(export_name_map, export_name_keep).transform)


    assert gdf_centers_processed.leyenda.nunique() <= 10, (
        'Carto legend will display some as "OTHERS"')


    # In[104]:



    # # Transform Areas

    # ## Joins

    # ### CAIPI - Municipalities

    # In[49]:


    caipi_areas_merged_gdf = caipi_pre_concat_assign_transformer.transform(
        caipi_areas_gdf) 

    # In[50]:


    gdf_caidminmujer_area.columns


    # ### Ministerio de la Mujer - Neighboorhood

    # In[51]:


    nonrepeated_cols = non_repeated_columns(gdf_caidminmujer_area, min_mujer_neigh_gdf, [],  ['index_right', 'left_index'],True)
    minmujer_areas_neigh_gdf = gpd.sjoin(min_mujer_to_merge_gdf, min_mujer_neigh_gdf[nonrepeated_cols])


    # ### CAIPI - Neighborhoods

    # In[52]:


    nonrepeated_cols = non_repeated_columns(caipi_areas_merged_gdf, caipi_barrios_gdf, ['latitud', 'longitud'], [], False)
    caipi_areas_neigh_gdf = caipi_areas_merged_gdf.merge(caipi_barrios_gdf[nonrepeated_cols], on =[ 'latitud', 'longitud'])

    caipi_areas_neigh_gdf = caipi_pre_concat_assign_transformer.transform(
        caipi_areas_neigh_gdf) 


    # merge conape capacities
    other_areas_merged_gdf = other_located_areas.merge(
        gdf_othcercenters[['level_0', 'itm', 'capacidad', 'has_epes']]).pipe(
        other_centers_pre_concat_select_transformer.transform)


    #TODO: variable nombr used as common key, a single key is required
    nonrepeated_cols = non_repeated_columns(other_areas_merged_gdf, other_centers_neigh_gdf, ['nombre'], [],False)
    other_areas_neigh_gdf = other_areas_merged_gdf.merge(other_centers_neigh_gdf[nonrepeated_cols], on =['nombre'])


    # ## Concatenate - Municipalities



    areas_concat_map = {
        # columns: caipi, caid & Min Mujer, others
        'name': ['centro', 'name', 'nombre'],
        'tipo': ['tipo', 'tipo', 'center_type'], 
        'itm': ['numcentro', 'itm', 'itm'],
        'capacidad': ['capacity', 'capacity', 'capacidad']
    }
    keep_features = ['municipality_key', 'geometry']

    concat_caipi_areas = rename_to_concat(
        caipi_areas_merged_gdf, 0, concat_map=areas_concat_map, keep_features = keep_features)
    concat_caid_min_mujer = rename_to_concat(
        gdf_caidminmujer_area, 1, concat_map=areas_concat_map, keep_features =keep_features)
    concat_other_areas = rename_to_concat(
        other_areas_merged_gdf, 2, concat_map=areas_concat_map, keep_features =keep_features)

    concat_areas = pd.concat((
    concat_caipi_areas, concat_caid_min_mujer, concat_other_areas), ignore_index=True)


    # ## Concatenate - Neighborhoods

    areas_concat_map = {
        # columns: caipi, caid & Min Mujer, others
        'name': ['centro', 'name', 'nombre'],
        'tipo': ['tipo', 'tipo', 'center_type'], 
        'itm': ['numcentro', 'itm', 'itm'],
        'capacidad': ['capacity', 'capacity', 'capacidad']
    }
    keep_features = ['municipality_key', 'geometry', 'neighborhood_key']

    concat_caipi_areas = rename_to_concat(
        caipi_areas_neigh_gdf, 0, concat_map=areas_concat_map, keep_features = keep_features)
    concat_caid_min_mujer = rename_to_concat(
        minmujer_areas_neigh_gdf, 1, concat_map=areas_concat_map, keep_features =keep_features)
    concat_other_areas = rename_to_concat(
        other_areas_neigh_gdf, 2, concat_map=areas_concat_map, keep_features =keep_features)

    concat_areas_neigh = pd.concat((
    concat_caipi_areas, concat_caid_min_mujer, concat_other_areas), ignore_index=True)

    # ## Export - Municipality

    export_assign_map = {

        'id': lambda df: df['tipo'].map(center_types_dict).astype(str).str.zfill(
            2).str.cat(df.itm.astype(str).str.zfill(4)),
        'municipio': lambda df: df['municipality_key'].str.cat(
            df['Municipio'].str.cat(df['Provincia'], sep=', '),
            sep=' - '),
        'nombre': lambda df: name_normalizer(df['name']).str.title(),
        'leyenda': lambda df: df['tipo']
    }

    areas_export_assign_map = dict(filter(lambda t: t[0] in [
        'id', 'municipio', 'nombre', 'leyenda'], export_assign_map.items()))



    areas_export_name_map = dict(filter(lambda t: not (t[0] in [
        'direccion']), export_name_map.items()))

    export_areas_name_keep = ['leyenda', 'nombre', 'id', 'capacidad', 'tipo', 'municipio', 'geometry', 'municipality_key']

    export_areas = concat_areas    .pipe(merge_municipalities.transform)    .pipe(SelectTransformer(export_select_map).transform)    .pipe(AssignTransformer(areas_export_assign_map).transform)    .pipe(ReplaceTransformer(export_replace_map, method='replace').transform)    .pipe(NameTransformer(areas_export_name_map, export_areas_name_keep).transform)


    # ## Export - Neighborhoods

    merge_neighborhoods = MergeTransformer(lambda: gdf_neighborhood_processed[[
        'municipality_key', 'neighborhood_key', 'Municipio', 'Provincia', 'TOPONIMIA']])


    areas_export_name_map_neigh = dict(filter(lambda t: not (t[0] in [
        'direccion']), export_name_map.items()))

    areas_export_assign_map_neigh= {

        'id': lambda df: df['tipo'].map(center_types_dict).astype(str).str.zfill(
            2).str.cat(df.itm.astype(str).str.zfill(4)),
        'municipio': lambda df: df['municipality_key'].str.cat(
            df['Municipio'].str.cat(df['Provincia'], sep=', '),
            
            sep=' - '),
        
        'barrio': lambda df: df['neighborhood_key'].str.cat(
                df['TOPONIMIA'].str.cat( 
                    df['municipio'],
                sep=', '), 
            sep=' - '),

        'nombre': lambda df: name_normalizer(df['name']).str.title(),
        'leyenda': lambda df: df['tipo']
    }

    export_areas_name_keep_neigh = ['leyenda', 'nombre', 'id', 'capacidad', 'tipo', 'municipio', 'geometry', 'barrio', 'municipality_key', 'neighborhood_key']

    gdf_areas_processed = concat_areas_neigh.pipe(merge_neighborhoods.transform)    .pipe(SelectTransformer(export_select_map_neigh).transform)    .pipe(AssignTransformer(areas_export_assign_map_neigh).transform)    .pipe(ReplaceTransformer(export_replace_map_neigh, method='replace').transform)    .pipe(NameTransformer(areas_export_name_map_neigh, export_areas_name_keep_neigh).transform)
    gdf_centers_processed['municipality_key'] = gdf_centers_processed.neighborhood_key.apply(lambda x: x[:4])

    return gdf_areas_processed, gdf_centers_processed