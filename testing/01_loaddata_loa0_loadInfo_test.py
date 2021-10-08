from loadData.loa1_loadInfo import load_data

def test_files_shape():
  #miembros_parsed_sample_df, household_parsed_df, municipalities_gdf, neighborhood_gdf = load_data()
  #assert miembros_parsed_sample_df.shape == (295612, 218)
  pass

def test_files_shape2():
  
  #miembros_parsed_sample_df, household_parsed_df, municipalities_gdf, neighborhood_gdf = load_data()
  #assert miembros_parsed_sample_df.shape == (295612, 218)
  pass

def test_load_data():
  ''' Check if the 
  '''
  centers_areas_walk, centers_areas_car, df_members, df_households, municipalities_gdf, neighborhood_gdf, areas_gdf, centers_gdf = load_data()
  assert df_members.shape== (463266, 218)
  assert df_households.shape== (146318, 106)
  assert municipalities_gdf.shape==(155, 5)
  assert neighborhood_gdf.shape==(12565, 7)
  assert centers_gdf.shape == (746, 17)
  assert centers_areas_walk.shape == (746, 2)
  assert centers_areas_car.shape == (746, 2)
  assert areas_gdf.shape == (668,11)
  