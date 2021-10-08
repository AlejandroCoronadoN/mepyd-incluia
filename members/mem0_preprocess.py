
from criteriaetl.transformers.columns_base import (NameTransformer, 
      ReplaceTransformer, SelectTransformer, AssignTransformer)
from criteriaetl.transformers.rows_base import (
      AggregateTransformer, DropTransformer)
from criteriaetl.transformers.fusion_base import MergeTransformer
import numpy as np
import pandas as pd

def members_date_transfomer(miembros_df):
    miembros_df["birth_date"] = pd.to_datetime(miembros_df['birth_date'])
    return miembros_df

def members_name_transfomer(miembros_parsed_df):
   '''Replace the name of the variables ans uses a more friendly reading name.
   This is also the list of variables from the SIUBEN survey that we are goin to be using through
   the entire pipeline. In case you want to add a new function then you will need 
   '''
   # member
   member_names_map = {
         'id': 'member_id',
         'cs12sexos': 'sexo',
         'cs12edads': 'edad',
         'cs12fechanacimientod': 'birth_date',
         'cs12parentescos': 'parentesco',
         'cs7noformularion': 'household_id',
         'cs14limitacionverv2': 'seeing', 
         'cs14limitacionescucharv2': 'listening',
         'cs14limitacioncaminarv2': 'walking', 
         'cs14limitacionrecordarconcentrarsev2.1': 'remembering', 
         'cs14limitacioncomunicarsev2': 'communicating',
         'cs14limitacioncuidadopersonalv2': 'personal_care',
         'cs14limitacionvivirindependientev2': 'independence',
         'cs14tandaasistev2': 'school_hours',
         'escuelacolegiotandaescolar': 'child_care_service',
         'cs18categoriaocupacionalv2': 'occupational_category',
         'nobuscotrabajo': 'inactivity_reason',
   }
   member_name_transfomrer = NameTransformer(
         member_names_map, keep_features=False)
   miembros_df = member_name_transfomrer.transform(miembros_parsed_df)
   return miembros_df


def member_assign_transformer(miembros_df):
   age_update_date_str = '2021-01-01'
   question_57 = ['seeing',
                                       'listening',
                                       'walking',
                                       'remembering',
                                       'communicating',
                                       'personal_care']
   
   question_58 = ['independence']

   assign_map = {
         'calculated_age': lambda df, date_str=age_update_date_str:\
         (pd.to_datetime(date_str) - df['birth_date']) / 
               np.timedelta64(1, 'Y'),
         'binned_age': lambda df: pd.cut(
               df['edad'], 
               bins=[-np.Inf] + list(range(9, 80, 10)) + [np.Inf], 
               labels=[f'{i} a {i + 9}' for i in range(0, 71, 10)] + ['80 o +']),
         'binned_age_5': lambda df: pd.cut(
               df['edad'], 
               bins=[-np.Inf] + list(range(4, 65, 10)) + [np.Inf], 
               labels=['4 o -'] + [f'{i} a {i + 9}' for i in range(5, 56, 10)] + ['65 o +']),
         'is_female': lambda df: df['sexo'].eq(2),
         'dificultad_cuidado_personal_leve': lambda df: df[
               'personal_care'].eq(2),
         'dificultad_cuidado_personal_grave': lambda df: df[
               'personal_care'].isin(range(3, 6)),
         'dependencia_leve': lambda df: df['independence'].eq(2), #& df[
               #'dificultad_cuidado_personal_leve'],
         'dependencia_grave': lambda df: df['independence'].isin(range(3, 5)),
         'dep_leve&dif_cp_leve': lambda df: (
               df['dependencia_leve'] & df['dificultad_cuidado_personal_leve']),
         'dep_grave&dif_cp_leve': lambda df: (
               df['dependencia_grave'] & df['dificultad_cuidado_personal_leve']),
         'dep_leve&dif_cp_grave': lambda df: (
               df['dependencia_leve'] & df['dificultad_cuidado_personal_grave']),
         'dep_grave&dif_cp_grave': lambda df: (
               df['dependencia_grave'] & df['dificultad_cuidado_personal_grave']),
         
         'discapacidad_visual': lambda df: df['seeing'].isin(range(2, 5)),
         'discapacidad_auditiva': lambda df: df['listening'].isin(range(2, 5)),
         'discapacidad_motriz': lambda df: df['walking'].isin(range(2, 5)),
         'discapacidad_memoria': lambda df: df['remembering'].isin(range(2, 5)),
         'discapacidad_comunicacional': lambda df: df['communicating'].isin(range(2, 5)),
         'member_0to4': lambda df: df['edad'].between(0, 4),
         'member_5to12': lambda df: df['edad'].between(5, 12),
         'tanda_escolar_incompleta': lambda df: df['school_hours'].ne(3),
         'no_recibe_ciudados_remunerados': lambda df: df['child_care_service'].eq(7),
         
         # care provision 0 to 4
         'recibe_ciudados_inaipi': lambda df: df['child_care_service'].eq(1) & df[
               'member_0to4'],
         'recibe_ciudados_publico_no_inaipi': lambda df: df[
               'child_care_service'].isin([2, 4, 5]) & df['member_0to4'],
         'recibe_ciudados_privado': lambda df: df['child_care_service'].eq(3) & df[
               'member_0to4'],
         'recibe_ciudados_otros': lambda df: df['child_care_service'].eq(6) & df[
               'member_0to4'],
         
         # superate care receivers
         'superate_visual': lambda df: df['discapacidad_visual'], # & df['independence'].isin([3, 4]),
         'superate_auditiva': lambda df: df['discapacidad_auditiva'], # & df['independence'].isin([3, 4]),
         'superate_motriz': lambda df: df['discapacidad_motriz'], # & df['independence'].isin([3, 4]),
         'superate_memoria': lambda df: df['discapacidad_memoria'], # & df['independence'].isin([3, 4]),
         'superate_comunicacional': lambda df: df['discapacidad_comunicacional'], # & df['independence'].isin([3, 4]),
         'superate_disability': lambda df:df[[
               'superate_visual', 'superate_auditiva', 'superate_motriz', 
               'superate_memoria', 'superate_comunicacional', 
         ]].any(1), 
         'superate_disability_13to64': lambda df: df['superate_disability'] & df[
               'edad'].between(13, 64),
         'superate_disability_5to64': lambda df: df['superate_disability'] & df[
               'edad'].between(5, 64),
         'superate_older_dependant': lambda df: df['independence'].isin(
               range(2, 5)) & df['edad'].ge(65),
         'superate_older_severe_dependant': lambda df: df['independence'].isin(
               range(3, 5)) & df['edad'].ge(65),
         'superate_child_0to4_receiver': lambda df: df[[
               'tanda_escolar_incompleta', 'no_recibe_ciudados_remunerados', 
               'member_0to4']].all(1),
         'superate_child_5to12_receiver': lambda df: df[[
               'tanda_escolar_incompleta', 'no_recibe_ciudados_remunerados', 
               'member_5to12']].all(1),
         'superate_care_receiver': lambda df: df[[
               'superate_disability_13to64', 'superate_older_dependant',   #Dependientes leves y graves
               'superate_child_0to4_receiver', 'superate_child_5to12_receiver',
         ]].any(1),
         'superate_care_receiver_wo_mild_dependant': lambda df: df[[
               'superate_disability_13to64', 'superate_older_severe_dependant', 
               'superate_child_0to4_receiver', 'superate_child_5to12_receiver',
         ]].any(1),
         'superate_care_receiver_wo_5to12': lambda df: df[[
               'superate_disability_5to64', 'superate_older_dependant', 
               'superate_child_0to4_receiver',
         ]].any(1),
         'superate_care_receiver_wo_both': lambda df: df[[
               'superate_disability_5to64', 'superate_older_severe_dependant', 
               'superate_child_0to4_receiver',
         ]].any(1),
         # superate care givers
         'superate_care_giver': lambda df: df['edad'].ge(18) & (~ df[ 
               'superate_care_receiver']) & df[question_57].eq(1).all(1) & df[
                     question_58].isin([1, np.nan]).all(1),
         'superate_care_giver_female': lambda df: df['is_female'] & df[
               'superate_care_giver'],
         'superate_care_giver_male': lambda df: df['sexo'].eq(1) & df[
               'superate_care_giver'],
         # domestic workers
         'woman_work_age': lambda df: df['is_female'] & df['edad'].ge(15),
         'man_work_age': lambda df: df['sexo'].eq(1) & df['edad'].ge(15),
         'domestic_worker': lambda df: df['occupational_category'].eq(6),
         'woman_domestic_worker': lambda df: df['woman_work_age'] & df[
               'domestic_worker'],
         'man_domestic_worker': lambda df: df['man_work_age'] & df[
               'domestic_worker'],
         'is_houseworker': lambda df: df['inactivity_reason'].eq(14),
         'woman_houseworker': lambda df: df['woman_work_age'] & df[
               'is_houseworker'],
         # family types
         'referente': lambda df: df['parentesco'].eq(1),
         'conyuge': lambda df: df['parentesco'].eq(2),
         'hijo': lambda df: df['parentesco'].isin(range(3, 6)),
         'otro_pariente': lambda df: df['parentesco'].isin(range(6, 16)),
         'no_pariente': lambda df: df['parentesco'].eq(16),
         'woman_led': lambda df: df['referente'] & df['is_female'],
         }
   dependance_assign_transformer = AssignTransformer(assign_map)
   members_ast_out = dependance_assign_transformer.transform(miembros_df)
   return members_ast_out

def members_aggregate_transformer(members_ast_out):
   sample_agg_map = {
         'any': [
               'superate_child_0to4_receiver', 'superate_child_5to12_receiver',
               'superate_disability_13to64', 'superate_disability_5to64',
               'superate_older_dependant', 'superate_older_severe_dependant',
               'superate_care_receiver', 'superate_care_receiver_wo_mild_dependant', 
               'superate_care_receiver_wo_5to12', 'superate_care_receiver_wo_both',
               'domestic_worker', 'woman_led', 'member_0to4'
         ],
         'sum': [
               'member_0to4', 'superate_child_0to4_receiver', 'member_5to12', 
               'superate_child_5to12_receiver', 'superate_disability_13to64',
               'superate_disability_5to64', 'recibe_ciudados_inaipi', 
               'recibe_ciudados_publico_no_inaipi', 'recibe_ciudados_privado', 
               'recibe_ciudados_otros',
               'superate_older_dependant', 'superate_older_severe_dependant', 
               'superate_care_receiver', 'superate_care_giver', 
               'superate_care_giver_female', 'superate_care_giver_male',
               'superate_care_receiver_wo_mild_dependant', 
               'superate_care_receiver_wo_5to12', 
               'superate_care_receiver_wo_both', 'woman_domestic_worker', 
               'woman_work_age', 'man_domestic_worker', 'man_work_age', 
               'woman_houseworker',
               'referente', 'conyuge', 'hijo', 'otro_pariente', 'no_pariente',
               'is_female'
         ],
         'count': ['member_id']
   }
   sample_aggregate_transformer = AggregateTransformer(sample_agg_map, [], groupby_='household_id')
   sample_agg_miembros_df = sample_aggregate_transformer.transform(members_ast_out)
   return sample_agg_miembros_df


if __name__ == "__main__":
    print('------------------- Runing mem0_preprocess.py -------------------')
