from households.hou0_preprocess import (
          household_replace_transformer, 
          household_name_transfomrer,
          create_municipality_neighborhood_keys,
          bernoulli_imputation
          )


import pandas as pd 
from projectetl.utils.config import data_dir
import numpy.testing as npt
#Load Initial DataFrame
df_households_start = pd.read_csv(f'data/testData/load/household_test.csv')
df_households_processed = pd.read_csv(f'data/testData/process/households_processed_test.csv')

def test_data():
  assert df_households_start.shape == (146318, 106)
  
def test_household_replace_transformer():
  df =   household_replace_transformer(df_households_start)
  test = df['cs8comerprimerov2'].unique() == [ True, False]
  assert test.all()
  
def test_municipality_neighborhood_keys():
  #household_df
  df_hnt  = household_name_transfomrer(df_households_start)
  assert df_hnt.shape == (146318, 12)
  
  #households_ast_out
  df_cmn = create_municipality_neighborhood_keys(df_hnt)
  assert df_cmn.shape == (146318, 14)
  npt.assert_array_equal( df_cmn.municipality_key.unique(),  ['3201'])
  npt.assert_array_equal( df_cmn.neighborhood_key.unique(), ['32010101025', '32010101010', '32010101015', '32010101020',
       '32010101003', '32010101008', '32010101026', '32010101013',
       '32010101016', '32010101027', '32010101004', '32010101009',
       '32010103005', '32010101011', '32010101002', '32010101028',
       '32010101014', '32010202003', '32010101021', '32010101019',
       '32010201001', '32010101001', '32010202001', '32010103004',
       '32010101005', '32010101017', '32010103001', '32010103003',
       '32010103002', '32010101007', '32010101006', '32010203001',
       '32010101012', '32010204002', '32010203003', '32010202002',
       '32010101018', '32010101024', '32010101023', '32010204001',
       '32010203002'])
  
  #There are no selected observations for the bernoulli imputation in Santo Domingo este
  
def test_household_benoulli():
  '''
  '''
  df_hrt = household_replace_transformer(df_households_start)
  #household_df
  df_hnt  = household_name_transfomrer(df_hrt)
  #households_ast_out
  df_cmn = create_municipality_neighborhood_keys(df_hnt)
  #bss_out -> superate_at_ou
  df_bi = bernoulli_imputation(df_cmn)
  

  assert df_bi.shape == (146318, 18)
  assert df_bi.icv_cat.mean() == 2.7560348834030455
  #There are no selected observations for the bernoulli imputation in Santo Domingo este
  assert df_bi.selected.sum() == 0
  # superate_or_cep 
  assert df_bi.superate_or_cep.sum() == 64992

  
def test_households_rodrigo_integration():
    assert df_households_start.shape ==  (146318, 106)

def test_households_data_end_rodrigo_integration():
    #household_rt_out
    household_parsed_df = household_replace_transformer(df_households_start)
    #household_df
    household_parsed_df  = household_name_transfomrer(household_parsed_df)
    #households_ast_out
    household_parsed_df = create_municipality_neighborhood_keys(household_parsed_df)
    #bss_out -> superate_dt_out
    household_parsed_df = bernoulli_imputation(household_parsed_df)
    assert household_parsed_df.shape ==  (146318, 18)

    
def test_describe_end_rodrigo_integration():
    df_desc = df_households_processed.describe()
    process_dict = df_desc.loc['mean'].to_dict()
    
    rodrigo_dict = {'icv_score': 83.76663970947266,
    'household_id': 4512883545693624.0,
    'lon': -4301051658240.0,
    'lat': 1716497809408.0,
    'province_code': 32.0,
    'municipality_code': 1.0,
    'neighborhood_code': 12.621844202353778,
    'section_code': 1.1944941839008187,
    'distmun_code': 1.0770991949042497,
    'ones': 1.0,
    'selected': 0.45699777197610686}
    