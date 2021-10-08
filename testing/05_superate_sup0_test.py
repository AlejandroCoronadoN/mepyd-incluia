import numpy.testing as npt 
import pandas as pd
from projectetl.utils.config import data_dir
from admindiv_aggroupation.adm0_superate import(get_admindiv_aggroupation)

df_neighborhood_grouped = pd.read_csv(f'data/testData/process/df_neighborhood_grouped_test.csv')
df_municipality_grouped = pd.read_csv(f'data/testData/process/df_municipality_grouped_test.csv')

def test_shape_neighborhood_end_rodrigo_integration():
    assert  df_neighborhood_grouped.shape == (41, 47)

def test_describe_neighbnorhood_end_rodrigo_integration():
    df_desc = df_neighborhood_grouped.describe()
    internal_dict = df_desc.loc['mean'].to_dict()
    external_dict =  {'member_0to4_sum': 1122.6097560975609,
    'recibe_ciudados_inaipi_sum': 13.292682926829269,
    'recibe_ciudados_publico_no_inaipi_sum': 17.097560975609756,
    'recibe_ciudados_privado_sum': 9.585365853658537,
    'recibe_ciudados_otros_sum': 111.7560975609756,
    'superate_child_0to4_receiver_sum': 954.5853658536586,
    'member_5to12_sum': 1767.658536585366,
    'superate_child_5to12_receiver_sum': 1183.878048780488,
    'superate_disability_13to64_sum': 537.219512195122,
    'superate_disability_5to64_sum': 575.2926829268292,
    'superate_older_dependant_sum': 124.34146341463415,
    'superate_older_severe_dependant_sum': 65.6829268292683,
    'woman_domestic_worker_sum': 270.4878048780488,
    'woman_work_age_sum': 4255.1463414634145,
    'man_domestic_worker_sum': 9.829268292682928,
    'man_work_age_sum': 3728.317073170732,
    'member_id_count': 11299.073170731708,
    'superate_child_0to4_receiver_any': 799.9024390243902,
    'superate_child_5to12_receiver_any': 882.3414634146342,
    'superate_disability_13to64_any': 452.3658536585366,
    'superate_disability_5to64_any': 477.8780487804878,
    'superate_older_dependant_any': 117.78048780487805,
    'superate_older_severe_dependant_any': 63.09756097560975,
    'superate_care_receiver_any': 1774.6341463414635,
    'superate_care_receiver_wo_mild_dependant_any': 1735.7317073170732,
    'superate_care_receiver_wo_5to12_any': 1290.7317073170732,
    'superate_care_receiver_wo_both_any': 1248.0975609756097,
    'domestic_worker_any': 272.8536585365854,
    'woman_houseworker_sum': 873.4878048780488,
    'woman_poor_sum': 2172.2926829268295,
    'is_female_sum': 5879.292682926829,
    'monomarental_with_superate_child': 303.9512195121951,
    'monomarental_with_superate_disability': 140.1219512195122,
    'monomarental_with_superate_elder_dependant': 26.0,
    'is_poor': 1289.121951219512,
    'household_id': 3568.682926829268,
    'dependence_0to4_rate': 0.5666847746572299,
    'dependence_5to12_rate': 0.6688696245049413,
    'dependence_dissability_13to64_rate': 0.7135365105025618,
    'dependence_dissability_5to64_rate': 0.7076247141355234,
    'dependence_older_dependant_rate': 0.672900721058581,
    'dependence_older_severe_dependant_rate': 0.6545712960113739,
    'dependence_rate_micro_wo_both': 0.6422557594435134,
    'dependence_rate_micro_wo_5to12': 0.6478332893186632,
    'dependence_rate_micro_wo_mild_dependant': 0.7208281681058327,
    'dependence_rate_micro': 0.7254463585179945}
        
    for k in internal_dict.keys():
        if k in external_dict.keys():
            assert internal_dict[k] == external_dict[k]



def test_shape_municipality_end_rodrigo_integration():
    assert  df_municipality_grouped.shape == (1, 47)

def test_describe_municipality_end_rodrigo_integration():
    df_desc = df_municipality_grouped.describe()
    internal_dict = df_desc.loc['mean'].to_dict()
    #TODO: Compare using dictionary from aws production
    external_dict = {'member_0to4_sum': 46027.0,
    'recibe_ciudados_inaipi_sum': 545.0,
    'recibe_ciudados_publico_no_inaipi_sum': 701.0,
    'recibe_ciudados_privado_sum': 393.0,
    'recibe_ciudados_otros_sum': 4582.0,
    'superate_child_0to4_receiver_sum': 39138.0,
    'member_5to12_sum': 72474.0,
    'superate_child_5to12_receiver_sum': 48539.0,
    'superate_disability_13to64_sum': 22026.0,
    'superate_disability_5to64_sum': 23587.0,
    'superate_older_dependant_sum': 5098.0,
    'superate_older_severe_dependant_sum': 2693.0,
    'woman_domestic_worker_sum': 11090.0,
    'woman_work_age_sum': 174461.0,
    'man_domestic_worker_sum': 403.0,
    'man_work_age_sum': 152861.0,
    'member_id_count': 463262.0,
    'superate_child_0to4_receiver_any': 32796.0,
    'superate_child_5to12_receiver_any': 36176.0,
    'superate_disability_13to64_any': 18547.0,
    'superate_disability_5to64_any': 19593.0,
    'superate_older_dependant_any': 4829.0,
    'superate_older_severe_dependant_any': 2587.0,
    'superate_care_receiver_any': 72760.0,
    'superate_care_receiver_wo_mild_dependant_any': 71165.0,
    'superate_care_receiver_wo_5to12_any': 52920.0,
    'superate_care_receiver_wo_both_any': 51172.0,
    'domestic_worker_any': 11187.0,
    'woman_houseworker_sum': 35813.0,
    'woman_poor_sum': 89064.0,
    'is_female_sum': 241051.0,
    'monomarental_with_superate_child': 12462.0,
    'monomarental_with_superate_disability': 5745.0,
    'monomarental_with_superate_elder_dependant': 1066.0,
    'is_poor': 52854.0,
    'household_id': 146316.0,
    'dependence_0to4_rate': 0.5565717560255251,
    'dependence_5to12_rate': 0.5989643625365805,
    'dependence_dissability_13to64_rate': 0.7185844502359507,
    'dependence_dissability_5to64_rate': 0.7117855306967275,
    'dependence_older_dependant_rate': 0.6742866261731646,
    'dependence_older_severe_dependant_rate': 0.6413027439546235,
    'dependence_rate_micro_wo_both': 0.6259612155942522,
    'dependence_rate_micro_wo_5to12': 0.6313256048229613,
    'dependence_rate_micro_wo_mild_dependant': 0.6922281653524548,
    'dependence_rate_micro': 0.6962034405272955}
            
    for k in internal_dict.keys():
        if k in external_dict.keys():
            assert internal_dict[k] == external_dict[k]
