
import pandas as pd 
import numpy as np 
from sklearn.preprocessing import MinMaxScaler
from criteriaetl.utils.display_func import (cdisplay, rdisplay, 
    percentage_count_plot)
import logging
import contextlib
import pandas.io.formats.format as pf
import geopandas as gpd
import pdb
# copied from https://stackoverflow.com/questions/29663252/
# can-you-format-pandas-integers-for-display-like-pd-options-display-float-forma
@contextlib.contextmanager
def df_formatting(float_fmt='{:0,.1f}', integer_fmt='{:,d}'):
    orig_float_format = pd.options.display.float_format
    orig_int_format = pf.IntArrayFormatter

    pd.options.display.float_format = float_fmt.format
    class IntArrayFormatter(pf.GenericArrayFormatter):
        def _format_strings(self):
            formatter = self.formatter or integer_fmt.format
            fmt_values = [formatter(x) for x in self.values]
            return fmt_values
    pf.IntArrayFormatter = IntArrayFormatter
    yield
    pd.options.display.float_format = orig_float_format
    pf.IntArrayFormatter = orig_int_format
    

def fillna_cat(srs): return srs.cat.add_categories("NA").fillna("NA")

def read_csv_idobject(file, geodata = False):
    '''Before reading a file as a csv this function allow us to detect id variables and parse them as objects
    This way we can avoid reading an string type as a numerical variable and have problems while making
    merge, join and testing operations'''
    try:
        
        if geodata:
            df = gpd.read_file(file, dtype = {'id': object, 'municipality_key':object, 'neighborhood_key':object})
        else:
            df = pd.read_csv(file, dtype = {'id': object, 'municipality_key':object, 'neighborhood_key':object})
            
    except Exception as e:
        logging.info(f' Could read variables in file: {file} trying with new format...')
        try:
            if geodata:
                df = gpd.read_file(file, dtype = { 'municipality_key':object, 'neighborhood_key':object})
            else:
                df = pd.read_csv(file, dtype = { 'municipality_key':object, 'neighborhood_key':object})
        except Exception as e:
            logging.info(f' Could read variables in file: {file} trying with new format...')
            try:
                if geodata:
                    df = gpd.read_file(file, dtype = { 'municipality_key':object, 'neighborhood_key':object})
                else:
                    df = pd.read_csv(file, dtype = { 'neighborhood_key':object})
            except Exception as e:
                logging.info(f' Could read variables in file: {file} trying with new format...')
                try:
                    if geodata:
                            df = gpd.read_file(file, dtype = {'municipality_key':object})
                    else:
                        df = pd.read_csv(file, dtype = {'municipality_key':object})
                except Exception as e:
                    logging.error(f' Could read variables in file: {file} the name of the file migh be worng -> error:', {e})
                    raise ValueError()
    return df
    

# on master its on projectetl.utils.display
def inspect_nulls(df):
    """Returns the number of nulls on all columns ehich present at least
     one null"""
    nulls_inspect = df.isnull().sum()
    return nulls_inspect[nulls_inspect > 0]
  
  
def format_varibles(df, n, percentaje_reporting_columns):
    ''' 
    Convert unreported values to 0. Unreported values happen when there are not SIUBEN households
    in agiven neighborhood.
    Reduce the number of decimals of the variable values to n points. 
    Converts to percentage scale the index variables
    '''
    df =  df.replace(np.nan, 0 )    
    for col in df.columns:
        try:
            df[col] = df[col].apply(lambda x: np.round(x, n))
        except Exception as e:
            print(e)
    for col in percentaje_reporting_columns:
        df[col] = df[col] * 100
        
    return df


def create_index_metric(df, weight_dict, metric_name = 'installed_cap_index'):
    for col in weight_dict.keys():
        df[col] = df[col].replace(np.inf, 0) 

    total_weight =0 
    df[metric_name] = 0 

    for i, col in enumerate(weight_dict):
        total_weight += weight_dict[col]
        df[metric_name] += df[col]* weight_dict[col]

    df[metric_name] = df[metric_name] /total_weight
    df.loc[(df[metric_name] > df[metric_name].quantile(.95)), metric_name] = df[metric_name].quantile(.95)

    df[metric_name]  = (df[metric_name]  -df[metric_name] .min())/df[metric_name].max()

    df['rank_' + metric_name] = df[metric_name].rank(ascending=False)

    return df


def non_repeated_columns(df1:pd.DataFrame, df2:pd.DataFrame, keep_columns:list = [], exclude_columns:list = [], include_geometry:bool =False):
    '''
    Returns a list with the names of the columns that apear in two dataframes
    with the same name. This is useful for merging with sjoin that dont have the
    pd.merge() functionality 
    
    keep_columns (list):
    include_geometry (bool) : If True include 'geometry column for sjoin'
    
    '''
    t1 = df1.columns
    t2 = df2.columns
    c2_n = [x for x in t2 if x not in t1]
    if include_geometry:
        c2_n.append('geometry')
        
    for k in keep_columns:
        if k in c2_n:
            continue
        else:
            c2_n.append(k)
            
    for e in exclude_columns:
        if e in c2_n:
            c2_n.remove(e)   
            
    return c2_n 

def shw(gdf):
    cdisplay(gdf.head(2), \
    gdf.shape, \
    gdf.plot())
    
def get_notmatching(gdf1, gdf2, col ='neighborhood_key'):
    '''
    col (str): must be a column that exists only in gdf2 so it returns NaN
    whenever we are unable to find an sjoin match for the associated 
    geometry variable in gf2
    '''
    gdf_join = gpd.sjoin(gdf1, gdf2, how='left', op='within')
    
    return gdf_join[gdf_join[col].isna()]

#shw(lostcaipi_neig_gdf)
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
        
    
def pivot_testing(df, index, columns, values = 'ones'):
    #Pivot table with margins to undesrtand distribution across categories.
    test = pd.pivot_table(df, \
                          values= values, \
                          index = index, \
                          columns = columns, \
                          aggfunc=np.sum, fill_value= 0,\
                         margins =True )
    cdisplay(test, test.shape, test.columns)
    return(test)

def get_unpairing_obs(df1, df2, merge_key):
    df = df1.merge(df2, on= merge_key, how='outer', indicator=True )
    onl = np.sum(df._merge =='left_only')
    onr = np.sum(df._merge =='right_only')
    print(f'Only left elements: {onl}, \nOnly right elements: {onr} \ndf merge shape: {df.shape} \ndf left shape: {df1.shape} \ndf right shape: {df2.shape}')
    return df

# on master its on projectetl.utils.display\n",
def inspect_nulls(df):
    """Returns the number of nulls on all columns ehich present at least
     one null"""
    nulls_inspect = df.isnull().sum()
    return nulls_inspect[nulls_inspect > 0]

def areaoverlay_capacity_assignment(df1, df2, df1_key, df2_key, capacity_col):
    ''' Returns the proportion of capacity_col that each df1(municipalities, neighborhood) entry
    covers for all the different df2 entries (influence area).
    Example:
    ----------
    df1  =  Neighborhoods with N neighborhoods
    df2 = Influence area of a particular center with C capacity
    Then we create a new variable capacity_area_assignment inside df1 with 
    the capacity C*n_i where n_i is the proportion of the influence area
    covered by the nth neighborhood.
    
    #*res_union contains all the pieces of the intersections in this case 
    #?    - Matches of df1 entry 1 with df2 entry 1
    #?    - Matches of df1 entry 1 with df2 entry 2
    #?    - Matches of df1 entry 2 with df2 entry 2
    #?    - Unmatched  entry 1 of df 1
    #?    - Unmatched  entry 2 of df 1
    #?    - Unmatched  entry 1 of df 2
    #?    - Unmatched  entry 2 of df 2
    #* df_intersection = gpd.overlay(df1_o, df2_o, how='intersection')
    #?    - Matches of df1 entry 1 with df2 entry 1
    #?    - Matches of df1 entry 1 with df2 entry 2
    #?    - Matches of df1 entry 2 with df2 entry 2
    
    '''
    #It cannot be the case that if a particular center doesn't report cpaacity then 
    #we don't share it's influence area. That's why we imputed 1 if the re not reported value
    #or if the capacity it's equal to 0
    df2.loc[df2[capacity_col]<=0, capacity_col] = 1
    df1_o = df1[[df1_key, 'geometry']].copy()
    df2_o = df2[[df2_key, 'geometry']].copy() #Influence area with capacity

    #We want to distribute the initial influence area into the differnet neighborhoods that overposition it
    df_intersection = gpd.overlay(df1_o, df2_o, how='intersection')
    df_intersection['intersection_area'] = df_intersection.area
    # A new influence area must be calculated since not all the influence areas match when we calculate the gdf.overlay
    df_intersection_newinfarea = df_intersection.groupby(df2_key).sum().reset_index()
    df_intersection_newinfarea =  df_intersection_newinfarea.rename(columns ={'intersection_area': 'new_influence_area'})
    
    #We are passiing the new_influence area at a df2_key (influence area 'id') aggregated level
    df_intersection = df_intersection.merge(df_intersection_newinfarea[[df2_key, 'new_influence_area']], on=df2_key)
    #Calaculate the proportions on the new influence area, this assignment should sum 1 
    df_intersection['proportion_newinfluence_area'] =  df_intersection['intersection_area']/  df_intersection['new_influence_area']
    
    #Now we are just passing the total capacity of the influence area to calculate the assigned capacity to the neighborhood
    df_intersection_capacity = df_intersection.merge(df2[[df2_key, capacity_col]], on = df2_key).reset_index()
    df_intersection_capacity['capacity_area_assignment'] = df_intersection_capacity['proportion_newinfluence_area'] * df_intersection_capacity[capacity_col]
    #n_l = ['32030203003', '32030203002', '32030203001', '32030201001
    #Check that the capacities match
    total_capacity_assignment =  df_intersection_capacity['capacity_area_assignment'].sum()
    total_capacity_df2 =  df2[capacity_col].sum()
    diff = np.abs((total_capacity_df2 - total_capacity_assignment)/total_capacity_assignment)
    if diff>.000001:
        #Something wrong happened while distributing the influence areas into the neighborhood or municipalities
        raise ValueError('!!! ERROR: geo0_functions/areaoverlay_capacity_assignment: Total capacity assigment do not match initial capacity')
    
    del df_intersection_capacity[capacity_col]
    df_intersection_capacity = df_intersection_capacity.rename(columns = {'capacity_area_assignment':capacity_col})
    #Now we just want to return a single observation for each df1_key (municipality/neighborhood)
    df_out= df_intersection_capacity.groupby(df1_key)[capacity_col].sum().reset_index()
    #raise ValueError()
    return df_out


def dissolve_dataframe_areas(df1, df2, id_col ='id'):
    ''' Takes two geoDataFrames df1 and df2 and creates a new geodataframe that dissolves both areas into a
    single polygon with the union of those two areas for each unique observation in df1 and df2 (defined by 
    id_col)
    Ex: Let df1 be a GeoDataFrame with a single observation that conatins the area of a 2x2 square and the coordinates
    (0,0), (0,2), (2,0), (2,2) and let df2 be a GeoDataFrame with a single observation that contains a 2x2 square in the coordiates
    (0,1), (0,3), (2,1), (2,3). This function will overlap the area of both square into a single dataframe with a single obsevtion that
    contains a polygon with the coordinates (0,0), (0,3), (2,0), (2,3)
    
    df1 (GeoDataFrame) : 
    '''
    df_dissolve = pd.DataFrame()
    for center in df1[id_col].unique():
        new_area = gpd.overlay(df1.loc[df1[id_col] == center], df2.loc[df2[id_col] == center], how = 'union')
        new_area[id_col] = center
        new_area = new_area.dissolve(by=id_col)
        if len(df_dissolve) ==0:
            df_dissolve = new_area
        else:
            df_dissolve = df_dissolve.append(new_area)
    df_dissolve = df_dissolve.reset_index()[[id_col, 'geometry']]
    return df_dissolve