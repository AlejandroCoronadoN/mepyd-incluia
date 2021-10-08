import numpy.testing as npt 
from projectetl.utils.config import data_dir
import pandas as pd

from members.mem0_preprocess import (
          members_name_transfomer,
          members_date_transfomer,
          member_assign_transformer,
          members_aggregate_transformer)

df_members = pd.read_csv(f'data/testData/load/miembros_test.csv')
df_members_processed = pd.read_csv(f'data/testData/process/members_processed_test.csv')


def test_members_start_rodrigo_integration():
    assert df_members.shape == (463266, 218)
    
def test_members_end_rodrigo_integration():
    #miembros_df 
    miembros_parsed_df = members_name_transfomer(df_members)
    #ExtraStep
    miembros_parsed_df = members_date_transfomer(miembros_parsed_df)
    #members_ast_out
    miembros_parsed_df = member_assign_transformer(miembros_parsed_df)
    #sample_agg_miembros_df
    miembros_parsed_df = members_aggregate_transformer(miembros_parsed_df)
    #TODO: We cant match members information with rodrigos data
    assert miembros_parsed_df.shape == (146318, 45)

#Members dont have an external validation because we haven done the municipality_key conversion to filter the testing data (municipality 3201)