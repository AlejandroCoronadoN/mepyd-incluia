from stats.sta0_createstats import (
      get_child_stats,
      get_elder_severe_stats,
      get_centers_stats,
      join_stats,
)
from projectetl.utils.config import data_dir
import geopandas as gpd
import pandas as pd 
import numpy as np

df_stats_processed_municipality = pd.read_csv(f'data/testData/process/stats_processed_municipality_test.csv')
df_stats_processed_neighborhood = pd.read_csv(f'data/testData/process/stats_processed_neighborhood_test.csv')

def test_shape_end_rodrigo_integration_neighborhood():
    assert df_stats_processed_neighborhood.shape ==(5428, 25)

def test_describe_end_rodrigo_integration_neighborhood():
    df_describe =  df_stats_processed_neighborhood.describe()
    internal_dict = df_describe.loc['mean'].to_dict() 
    external_dict = {'capacidad total (CAIPI)': 15.27708881326607,
    '# COS INFOTEP': 0.09073324571984259,
    'capacidad total (ASFL DIURNAS)': 4.4624635386248706,
    'capacidad total (ESTANCIAS CONAPE)': 0.8415463067949944,
    '# CCPP SIN EPES': 0.03319273654433066,
    '# Oficina de la Mujer': 0.017331092872348547,
    '# Direccion Regional PROSOLI': 0.017401266197889002,
    'capacidad total (CTC CON EPES)': 1.7651828558491778,
    'capacidad total (CCPP CON EPES)': 7.550210006630885,
    'capacidad total (Nuevos C. de Dia CONAPE)': 0.850233863502792,
    '# Direccion Regional INFOTEP': 0.01666493024123255,
    'capacidad total (ASFL PERMANENTES)': 0.7957256936304248,
    '# CAID': 0.012267369292637055,
    'capacidad total (TOTAL CONAPE)': 1.5674437421213436}
        
    for k in internal_dict.keys():
        if k in external_dict.keys():
            if external_dict[k] ==0:
                diff = internal_dict[k]
            else:
                diff = np.abs((internal_dict[k]-external_dict[k])/ external_dict[k])
            assert diff< .00001

def test_shape_end_rodrigo_integration_municipality():
    assert df_stats_processed_municipality.shape ==(141, 25)

def test_describe_end_rodrigo_integration_municipality():
    df_describe =  df_stats_processed_municipality.describe()
    internal_dict = df_describe.loc['mean'].to_dict() 
    external_dict = {'capacidad total (CAIPI)': 491.23444593921704,
    '# COS INFOTEP': 2.9403997005472586,
    'capacidad total (ASFL DIURNAS)': 150.19751144352398,
    'capacidad total (ESTANCIAS CONAPE)': 20.54922215871754,
    '# CCPP SIN EPES': 0.8788984263629198,
    '# Oficina de la Mujer': 0.5577041799018801,
    '# Direccion Regional PROSOLI': 0.46303634627591356,
    'capacidad total (CTC CON EPES)': 35.842450807519505,
    'capacidad total (CCPP CON EPES)': 148.2903476653494,
    'capacidad total (Nuevos C. de Dia CONAPE)': 17.513701823737165,
    '# Direccion Regional INFOTEP': 0.49897362354744274,
    'capacidad total (ASFL PERMANENTES)': 23.67121042094496,
    '# CAID': 0.3326001311254696,
    'capacidad total (TOTAL CONAPE)': 60.352663180426866,
    'accesible_child_0to4_receiver_caipi': 21272.0,
    'accesible_child_0to4_receiver_ccpp_epes': 7790.0,
    'accesible_elder_asfl_diurna': 2831.0,
    'accesible_elder_asfl_permanente': 1781.0,
    'accesible_elder_estancias_conape': 1795.0,
    'accesible_elder_nuevos_conape': 1179.0,
    'accesible_severe_asfl_diurna': 1512.0,
    'accesible_severe_asfl_permanente': 926.0,
    'accesible_severe_estancias_conape': 941.0,
    'accesible_severe_nuevos_conape': 627.0}
    
    for k in internal_dict.keys():
        if k in external_dict.keys():
            if external_dict[k] ==0:
                diff = internal_dict[k]
            else:
                diff = np.abs((internal_dict[k]-external_dict[k])/ external_dict[k])
            assert diff< .00001
   