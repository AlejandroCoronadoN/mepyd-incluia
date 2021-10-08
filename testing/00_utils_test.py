from geofunctions.geo0_functions import areaoverlay_capacity_assignment
import numpy.testing as npt
import geopandas as gpd 
from shapely.geometry import Polygon

def test_areaoverlay_capacity_assignment():
    
    polys1 = gpd.GeoSeries([
                        Polygon([(0,0), (2,0), (2,2), (0,2)]), 
                        Polygon([(2,0), (4,0), (4,2), (2,2)]), 
                        Polygon([(0,2), (2,2), (2,4), (0,4)]), 
                        Polygon([(2,2), (4,2), (4,4), (2,4)]),
                        
                        Polygon([(4,0), (8,0), (8,4), (4,4)]), 
                        Polygon([(4,4), (4,8), (0,8), (0,4)]), 
                        Polygon([(4,4), (8,4), (8,8), (4,8)]), 
                        
                        ])
    #polys1.plot()

    polys2 = gpd.GeoSeries([
        Polygon([(1,1), (3,1), (3,3), (1,3)]),
        Polygon([(2,2), (2,6), (6,6), (6,2)]),
                                ])
    #polys2.plot()

    df1 = gpd.GeoDataFrame({'geometry': polys1, 'neighborhood_key':[1,2,3,4,5,6,7]})
    df2 = gpd.GeoDataFrame({'geometry': polys2, 'area_key':[1,2], 'capacity':[10,100]})

    capacity_col =  'capacity'
    df1_key = 'neighborhood_key'
    df2_key = 'area_key'

    result = areaoverlay_capacity_assignment(df1, df2, df1_key, df2_key, capacity_col)
    npt.assert_array_equal(result['capacity_area_assignment'], [ 2.5,  2.5,  2.5, 27.5, 25. , 25. , 25. ])
    pass