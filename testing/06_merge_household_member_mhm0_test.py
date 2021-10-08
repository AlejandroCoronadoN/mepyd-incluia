import numpy.testing as npt 
from projectetl.utils.config import data_dir
import pandas as pd
import geopandas as gpd
from projectetl.utils.dataload import load_s3_data_do, get_path_from_pattern
import numpy as np 
from merge_household_member.mhm0_merge import (
          mem_hous_merge_transformer,
          mem_hous_merge_select_transformer,
          mem_hous_merge_assign_trasnformer,
          mem_hous_merge_drop_transformer,
          )

#df_households_processed = pd.read_csv(f'data/testData/process/households_processed_test.csv')
#df_members_processed =  pd.read_csv(f'data/testData/process/members_processed_test.csv')
#superate_dt_out
df_households_members_processed =  pd.read_csv(f'data/testData/process/households_members_processed_test.csv')
df_households_members_processed_pipeline = pd.read_csv(f'data/pipeline/process/households_member_area_processed_pipeline.csv')


def test_shape_end_data_rodrigo_integration():    
    assert df_households_members_processed.shape == (146316, 79)
    
def test_describe_end_rodrigo_integration():
    df_describe =  df_households_members_processed.describe()
    internal_dict = df_describe.loc['mean'].to_dict() 
    external_dict = {'household_id': 4512831214293602.0,
    'member_0to4_sum': 0.3145725689603324,
    'superate_child_0to4_receiver_sum': 0.26748954318051343,
    'member_5to12_sum': 0.4953251865824653,
    'superate_child_5to12_receiver_sum': 0.3317408895814538,
    'superate_disability_13to64_sum': 0.15053719347166408,
    'superate_disability_5to64_sum': 0.16120588315700266,
    'recibe_ciudados_inaipi_sum': 0.003724814784439159,
    'recibe_ciudados_publico_no_inaipi_sum': 0.0047910003007189915,
    'recibe_ciudados_privado_sum': 0.002685967358320348,
    'recibe_ciudados_otros_sum': 0.03131578227944996,
    'superate_older_dependant_sum': 0.03484239591022171,
    'superate_older_severe_dependant_sum': 0.018405369200907625,
    'superate_care_receiver_sum': 0.784610022143853,
    'superate_care_giver_sum': 1.8284944913748327,
    'superate_care_giver_female_sum': 0.9656018480548949,
    'superate_care_giver_male_sum': 0.8628926433199376,
    'superate_care_receiver_wo_mild_dependant_sum': 0.768172995434539,
    'superate_care_receiver_wo_5to12_sum': 0.46353782224773776,
    'superate_care_receiver_wo_both_sum': 0.4471007955384237,
    'woman_domestic_worker_sum': 0.0757948549714317,
    'woman_work_age_sum': 1.1923576368954865,
    'man_domestic_worker_sum': 0.0027543125837229013,
    'man_work_age_sum': 1.044731950025971,
    'woman_houseworker_sum': 0.24476475573416442,
    'referente_sum': 1.0,
    'conyuge_sum': 0.4653284671532847,
    'hijo_sum': 1.3117362421061265,
    'otro_pariente_sum': 0.3797875830394489,
    'no_pariente_sum': 0.00932228874490828,
    'is_female_sum': 1.6474684928510894,
    'member_id_count': 3.1661745810437685,
    'icv_cat': 2.7560348834030455,
    'icv_score': 83.76422235230442,
    'lon': -4301111552877.776,
    'lat': 1716521745176.1753,
    'province_code': 32.0,
    'municipality_code': 1.0,
    'neighborhood_code': 12.621852702370212,
    'section_code': 1.1944968424505864,
    'distmun_code': 1.0771002487766204,
    'ones': 1.0,
    'selected': 0.0,
    'dependence_0to4_rate': 0.160756762814294,
    'dependence_5to12_rate': 0.20598779842453904,
    'dependence_dissability_13to64_rate': 0.12353469464004313,
    'dependence_dissability_5to64_rate': 0.1309433170736793,
    'dependence_older_dependant_rate': 0.02832416530952589,
    'dependence_older_severe_dependant_rate': 0.01456490453148369,
    'dependence_rate_micro': 0.518603421188408,
    'dependence_rate_micro_wo_mild_dependant': 0.5048441604103687,
    'dependence_rate_micro_wo_5to12': 0.3200242451975084,
    'dependence_rate_micro_wo_both': 0.3062649844194651,
    'woman_poor_sum': 0.6087099155253014}
    
    for k in internal_dict.keys():
        if k in external_dict.keys():
            assert internal_dict[k] == external_dict[k]
            
def test_describe_end_rodrigo_integration_pipeline():
    df_describe =  df_households_members_processed_pipeline.describe()
    internal_dict = df_describe.loc['mean'].to_dict() 
    external_dict = {'lat': 18.64450907423051,
    'lon': -70.01375743964404,
    'municipality_key': 1752.2785542597885,
    'neighborhood_key': 17522895363.51458,
    'member_0to4_sum': 0.273333538095716,
    'recibe_ciudados_inaipi_sum': 0.008691341355567378,
    'recibe_ciudados_publico_no_inaipi_sum': 0.005183227577671628,
    'recibe_ciudados_privado_sum': 0.0017529782911127338,
    'recibe_ciudados_otros_sum': 0.029508690468204937,
    'superate_child_0to4_receiver_sum': 0.22365506811037159,      
    'member_5to12_sum': 0.4779019443626365,
    'superate_child_5to12_receiver_sum': 0.3036517932818969,      
    'superate_disability_13to64_sum': 0.14373774828423333,        
    'superate_disability_5to64_sum': 0.15459624949960765,
    'superate_older_dependant_sum': 0.04234626462565824,
    'superate_older_severe_dependant_sum': 0.021402873651714185,
    'woman_domestic_worker_sum': 0.07993262564303692,
    'woman_work_age_sum': 1.1632810165445608,
    'man_domestic_worker_sum': 0.0029982965344857006,
    'man_work_age_sum': 1.0433899364356616,
    'member_id_count': 3.084057800106237,
    'household_id': 4496796569025069.5,
    'dependence_0to4_rate': 0.13244900128135217,
    'dependence_5to12_rate': 0.18894541997005995,
    'dependence_dissability_13to64_rate': 0.11651653518288653,
    'dependence_dissability_5to64_rate': 0.12393469940923492,
    'dependence_older_dependant_rate': 0.03463728278537507,
    'dependence_older_severe_dependant_rate': 0.017232986538938923,
    'dependence_rate_micro_wo_both': 0.27361668722969046,
    'dependence_rate_micro_wo_5to12': 0.2910209834763265,
    'dependence_rate_micro_wo_mild_dependant': 0.45514394297488475,
    'dependence_rate_micro': 0.47254823922108713,
    'woman_houseworker_sum': 0.25298368410190963,
    'woman_poor_sum': 0.8672453566620006,
    'is_female_sum': 1.5920417353673566,
    'index_right': 324.3653877543809,
    'id': 59100.21987268231,
    'capacidad': 176.2536471998194}
    for k in internal_dict.keys():
        if k in external_dict.keys():
            if external_dict[k] ==0:
                diff = internal_dict[k]
            else:
                diff = np.abs((internal_dict[k]-external_dict[k])/ external_dict[k])
            assert diff< .00001
