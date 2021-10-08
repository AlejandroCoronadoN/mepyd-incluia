import numpy.testing as npt 
from projectetl.utils.config import data_dir
import pandas as pd
import geopandas as gpd
from projectetl.utils.dataload import load_s3_data_do, get_path_from_pattern

from merge_household_member_area.hma0_merge import (
          mem_hous_merge_name_transformer,
          to_geodata,
          merge_mem_hous_areas,
          )

#merged_superate_gdf
df_households_members_area = pd.read_csv(f'data/testData/process/households_member_area_processed_test.csv')
    
    
def test_shape_end_data_rodrigo_integration():    
    assert df_households_members_area.shape == (2946823, 60)
    
def test_describe_end_rodrigo_integration():
    df_describe =  df_households_members_area.describe()
    internal_dict = df_describe.loc['mean'].to_dict() 
    external_dict = {'lat': 18.501012261349672,
    'lon': -69.85537822495542,
    'member_0to4_sum': 0.2863331119649874,
    'recibe_ciudados_inaipi_sum': 0.004118672889413446,
    'recibe_ciudados_publico_no_inaipi_sum': 0.006666840865569463,
    'recibe_ciudados_privado_sum': 0.003557390450665004,
    'recibe_ciudados_otros_sum': 0.02613662238960399,
    'superate_child_0to4_receiver_sum': 0.24223104000477802,
    'member_5to12_sum': 0.46368478866901747,
    'superate_child_5to12_receiver_sum': 0.34985304512690446,
    'superate_disability_13to64_sum': 0.15603855406313852,
    'superate_disability_5to64_sum': 0.16698627640682864,
    'superate_older_dependant_sum': 0.04262217309963985,
    'superate_older_severe_dependant_sum': 0.02260705851691805,
    'woman_domestic_worker_sum': 0.07061469250104264,
    'woman_work_age_sum': 1.2042464036693077,
    'man_domestic_worker_sum': 0.0017907420975063653,
    'man_work_age_sum': 1.0344051882315293,
    'member_id_count': 3.1018045535819425,
    'household_id': 4519131541502681.0,
    'dependence_0to4_rate': 0.14503762847115753,
    'dependence_5to12_rate': 0.2185394890894973,
    'dependence_dissability_13to64_rate': 0.1275173139246398,
    'dependence_dissability_5to64_rate': 0.1352092813983006,
    'dependence_older_dependant_rate': 0.03411360463858577,
    'dependence_older_severe_dependant_rate': 0.017606216851812184,
    'dependence_rate_micro_wo_both': 0.29785312672159225,
    'dependence_rate_micro_wo_5to12': 0.31436051450840496,
    'dependence_rate_micro_wo_mild_dependant': 0.5087006483367968,
    'dependence_rate_micro': 0.5252080361235202,
    'woman_houseworker_sum': 0.23730980788462694,
    'woman_poor_sum': 0.5314177336066672,
    'is_female_sum': 1.6260912175587063,
    'index_right': 285.7596611672978,
    'capacidad': 140.60905129168023}
    
    for k in internal_dict.keys():
        if k in external_dict.keys():
            assert internal_dict[k] == external_dict[k]
   