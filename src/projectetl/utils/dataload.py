import pandas as pd
from dotenv import load_dotenv
import os
import glob

# point to covidsource
from covidsource.utils.dataload import load_survey_data, load_s3_data
from covidsource.utils.common_func import copy_docstring
from projectetl.utils.config import SURVEY_DATA_PATH, \
    S3_BUCKET_NAME, S3_KEY_DICT, LOCAL_LOAD_FUNC, S3_LOAD_FUNCTIONAL


@copy_docstring(load_survey_data)
def load_survey_data_sv(path=SURVEY_DATA_PATH,
                        load_func=LOCAL_LOAD_FUNC,
                        columnnames_to_lower=True,
                       ) -> pd.DataFrame:
    return load_survey_data(path, load_func,
                            columnnames_to_lower=columnnames_to_lower)


@copy_docstring(load_s3_data)
def load_s3_data_do(key_str,
                    bucket_name=S3_BUCKET_NAME,
                    key_dict=S3_KEY_DICT,
                    load_functional=S3_LOAD_FUNCTIONAL,
                    columnnames_to_lower=True,
                    **load_kwargs
                    ):
    load_dotenv()
    s3_profile_name = os.getenv('AWS_PROFILE_NAME')
    s3_key = key_dict[key_str]
    print(
        f'''
        reading with AWS profile name: {s3_profile_name}
        reading with key: {s3_key}
        ''')


    return load_s3_data(s3_profile_name, bucket_name, s3_key,
                        load_functional(s3_key, **load_kwargs),
                        columnnames_to_lower=columnnames_to_lower)


def get_path_from_pattern(pattern, i=-1, verbose=True):
    """Returns `i`th file matching with pattern"""
    path = sorted(glob.glob(pattern))[i]
    if verbose:
        print(f'found path: {path}')
    return path