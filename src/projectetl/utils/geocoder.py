# Roodrigo Lara Molina - 18/07/2018
import time
from geopy import geocoders
# from tqdm import tqdm
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm_notebook as tqdm
import pandas as pd
import numpy as np


class Geocoder():
    """ 
    This class allows to geocode an iterable, of addresses in string 
    format and to respect a time between quries.
    
    ARGUMENTS:
    order   ->  list (optional). If provided, list of geopy.geocoder 
                instances, if not, a default order is followed. Default None.
    sleep   ->  int (optional). Specifies the time to wait between each query.
    """
    
    def __init__(self, order=None, sleep=2):
        if order is None:
            self.geocoders = [
                #geocoders.Nominatim(user_agent="", proxies={},
                #timeout=5),
                geocoders.GoogleV3(
                    api_key="AIzaSyApTjSe07NheKK0R2kj3TnP57NK-Wk1zoM",
                    timeout=10)
                #,geocoders.Yandex(lang='en_US', proxies={})
            ]
        else:
            self.geocoders =  order
        self.sleep = sleep


    def geocode(self, df, col_to_geocode, debug=2, **kwargs):
        """
        Method which allows to go through an iterable of query strings 
        and get their longitude and latitude, passing by various geocoder API's

        ARGUMENTS:
        iterable    ->  iterable of str
        
        RETURNS:
        address_dict->  dict, {str: list} with keys 'address', 
        'longitude', 'latitude',
        """
        # print(kwargs)
        # raise ValueError
        address_df = df.copy()
        provider_dict = dict(zip([
            # 'https://nominatim.openstreetmap.org/search',
            'https://maps.googleapis.com/maps/api/geocode/json'#,
            #'https://geocode-maps.yandex.ru/1.x/'
            ], [#'osm',
                'gmaps'#, 'yandex'
            ]))

        # monitor progress with tqdm
        error_counter = 0
        postfix_dict = {
            "total number of success": 0,
            "gmaps": 0, #"osm": 0,
            "total number of errors": 0
        }
        _total = len(df[col_to_geocode]) if not debug else debug
        prog_monitor = tqdm(enumerate(df[col_to_geocode].iteritems()), desc='geocoding',
            total=_total, postfix=postfix_dict)

        complement_raw_list = []
        complement_index_list = []
        for i, (idx, query) in prog_monitor:
            
            for j, gc in enumerate(self.geocoders):
                try:
                    #print('trying', provider_dict[gc.api])
                    for col, val in kwargs.items():
                        address_df.loc[idx, col] = val if (not isinstance(val, dict)) else str(val)
                    # print('ok kwargs')
                    time.sleep(self.sleep)
                    res = gc.geocode(query, **kwargs)
                    address_df.loc[idx, 'address'] = res.address
                    address_df.loc[idx, 'longitude'] = res.longitude
                    address_df.loc[idx, 'latitude'] = res.latitude
                    complement_raw_list.append(res.raw)
                    complement_index_list.append(idx)
                    # address_df.loc[idx, 'query'] = query

                except Exception as ex:
                    if debug:
                        print(provider_dict[gc.api], f'{ex.__class__.__name__}: {ex}')
                        if i >= debug:
                            complement = pd.json_normalize(complement_raw_list)
                            complement.index = pd.MultiIndex.from_tuples(
                                complement_index_list, names=address_df.index.names)
                            # for col, val in pd.json_normalize(res.raw).loc[
                            #     0].iteritems():
                            #     address_df.loc[idx, col] = val

                            return address_df.assign(**complement)

                    if j <  len(self.geocoders) - 1:
                        #print(provider_dict[gc.api], ex)
                        continue
                    else:
                        #print(provider_dict[gc.api], ex)
                        #print(ex.message)
                        pass

                else: #  this is excecuted if the try script raise no exceptions
                    if debug:
                        print([query, res.longitude, res.latitude, gc.api])
                        if i >= debug:
                            complement = pd.json_normalize(complement_raw_list)
                            complement.index = pd.MultiIndex.from_tuples(
                                complement_index_list,
                                names=address_df.index.names)
                            return address_df.assign(**complement)

                    # update postfix_dict
                    postfix_dict[provider_dict[gc.api]] += 1
                    #prog_monitor.set_description(f'success with {provider_dict[gc.api]}')
                    break

            else:
                #address_df.loc[idx, 'parsed_query'] = query
                # address_df.loc[idx, 'address'] = None
                address_df.loc[idx, 'longitude'] = np.nan
                address_df.loc[idx, 'latitude'] = np.nan
                # address_df.loc[idx, 'provider'] = None
                error_counter += 1
                #prog_monitor.set_description(f'ERROR: total number of errors: {error_counter}')

            # update tqdm
            postfix_dict.update({
                        "total number of success": i - error_counter, "total number of errors": error_counter
                        })
            prog_monitor.set_postfix(postfix_dict)

        complement = pd.json_normalize(complement_raw_list)
        complement.index = pd.MultiIndex.from_tuples(
            complement_index_list, names=address_df.index.names)
                
        return address_df.assign(**complement)
            