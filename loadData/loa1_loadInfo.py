import pandas as pd
import geopandas as gpd
from projectetl.utils.config import data_dir
from projectetl.utils.dataload import load_s3_data_do, get_path_from_pattern

def load_data( TESTING    = True):
    ''' Load al the DataSets that will be used in the inluia pipeline. 
    centers_areas_walk: For each care center we created a influence are delimited by 30 mintues
    walking distance.
    centers_areas_car: For each care center we created a influence are delimited by 30 mintues
    walking distance.
    gdf_centers:
    gdf_areas:
    gdf_neighborhood:
    gdf_municipality:
    df_households:
    df_members:
    
    '''
    centers_areas_walk = gpd.read_file( f'data/raw/centers_areas_walk.geojson')
    centers_areas_walk = gpd.GeoDataFrame(centers_areas_walk, crs="EPSG:4326", geometry='geometry')
    centers_areas_walk = centers_areas_walk[['id','geometry']]

    centers_areas_car = gpd.read_file( f'data/raw/centers_areas_car.geojson')
    centers_areas_car = gpd.GeoDataFrame(centers_areas_car, crs="EPSG:4326", geometry='geometry')
    centers_areas_car =centers_areas_car[['id','geometry']]

    #Municipalities
    #TODO: Since we are using the same municipality, neighbor and areas files for both testing and not testing.
    munis_geojson_path = (f'data/preprocess/preprocess_municipality.geojson')
        
    if not TESTING: 
        #Household and members all data
        df_households = load_s3_data_do('hogares')
        df_members = load_s3_data_do('miembros')

    else:
        df_members = pd.read_csv(f'data/raw/miembros_raw_test.csv')
        df_households =pd.read_csv(f'data/raw/households_raw_test.csv')

    dissolved_influence_areas = False
    return centers_areas_walk, centers_areas_car, df_members, df_households


if __name__ == "__main__":
    print('------------------- Runing loa1_loadinfo.py -------------------')