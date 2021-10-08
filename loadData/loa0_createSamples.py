import itertools as it
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
from shapely.geometry import Point

from projectetl.utils.config import data_dir
from projectetl.utils.dataload import load_s3_data_do

def plot_maps(gdf1, gdf2, gdf3 =None, gdf4=None, gdf5=None):
    fig, ax1 = plt.subplots(figsize=(5, 3.5))
    gdf1.plot(ax=ax1)
    try :
        gdf2.plot(ax=ax1, color = 'red')
    except Exception as e:
        print('')
    try :
        gdf3.plot(ax=ax1, color = 'green')
    except Exception as e:
        print('')

    try :
        gdf4.plot(ax=ax1, color = 'purple')
    except Exception as e:
        print('')

    try :
        gdf5.plot(ax=ax1, color = 'orange')     
    except Exception as e:
        print('')
        
#---------------------------HOUSEHOLDS  SAMPLE --------------------------------

household_parsed_df = load_s3_data_do('hogares')

# Santo Domingo Este municipality_key 3201
household_parsed_df = household_parsed_df[(household_parsed_df['cs1provinciav2'] == 32)&
                                          (household_parsed_df.cs1municipiov2 ==1) ]

household_parsed_df.to_csv(f'data/testData/load/household_test.csv', index = False)

#geometry_cols = {
#    'cs_longitudcoordenadav250': 'lon',
#    'cs_latitudcoordenadav250': 'lat',}
#
#household_parsed_df = household_parsed_df.rename(columns = geometry_cols)
#household_parsed_df = household_parsed_df[(household_parsed_df.lon != -1)& (household_parsed_df.lon != -1)]
#household_parsed_df.loc[:, 'geometry'] = household_parsed_df[['lon', 'lat']].apply(lambda *args: Point(tuple(*args)), axis=1)
#    
#household_parsed_df = gpd.GeoDataFrame(household_parsed_df, crs="EPSG:4326", geometry='geometry')

#---------------------------MUNCIPALITIES  SAMPLE --------------------------------

#Call municipalities geojson to verify that the places where the households land are inside the municipality 3201
cols_municipalities_gpd = [
    'Provincia', 'Municipio', 'municipality_key', 'caipi_sum',
    'geometry'
]
munis_geojson_path = (
    f'data/geo/parsed/municipality/recodified_municipalities-'
    f'withCAIPIs-s9e-05.geojson')

municipalities_gdf = gpd.read_file(
    munis_geojson_path)[cols_municipalities_gpd].copy()

municipalities_sample_gdf =  municipalities_gdf[municipalities_gdf.municipality_key == "3201"]
#Note that we are using all municipalities for testing
municipalities_gdf.to_csv(f'data/testData/load/municipalities_test.csv', index = False)

#All points from the households id must match the municipality 3201 Santo Domingo este
#plot_maps(municipalities_sample_gdf, household_parsed_df[['cs1noformularion', 'geometry']].sample(100))

#---------------------------NEIGHBORHOODS  SAMPLE --------------------------------

cols_neighborhood_gpd = [
    'Provincia', 'TOPONIMIA', 'neighborhood_key', 'caipi_sum_neighbor',
    'geometry','municipality_key', 'Municipio'
]
neigh_geojson_path = (
    f'data/preprocess/preprocess_neighborhood.geojson')
neighborhood_gdf = gpd.read_file(
    neigh_geojson_path)[cols_neighborhood_gpd].copy()
neighborhood_gdf['TOPONIMIA'] = neighborhood_gdf['TOPONIMIA'].str.title()

#Note that we are using all neighborhood_gdf for testin. 
neighborhood_gdf.to_csv(f'data/testData/load/neighborhoods_test.csv', index = False)

#---------------------------SIUBEN MEMEBERS SAMPLE --------------------------------

miembros_parsed_df = load_s3_data_do('miembros')

#We are going to use the number of formulario to match it with the household id it belongs to cs7noformularion - cs1noformularion
miembros_parsed_sample_df = miembros_parsed_df.merge(household_parsed_df[['cs1noformularion', 'cs1municipiov2']], left_on = 'cs7noformularion', right_on = 'cs1noformularion')
#All selected members belong to municipality 1
assert np.sum(miembros_parsed_sample_df.cs1municipiov2 ==1) == len(miembros_parsed_sample_df)
del miembros_parsed_sample_df['cs1municipiov2']

miembros_parsed_sample_df.to_csv(f'data/testData/load/miembros_test.csv', index = False)

