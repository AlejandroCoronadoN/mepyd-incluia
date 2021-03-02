import pickle
import pandas as pd

# criteriaetl imports
from criteriaetl.utils.dataload import load_survey_data, load_s3_data
from criteriaetl.utils.common_func import copy_docstring

from projectetl.utils.config import (
    S3_PROFILE_NAME, S3_BUCKET_NAME, S3_KEY, LOCAL_LOAD_FUNC, S3_LOAD_FUNC)


@copy_docstring(load_survey_data)
def load_survey_data_do(path,
                        load_func=LOCAL_LOAD_FUNC,
                        columnnames_to_lower=True):
    data = load_func(path)

    if columnnames_to_lower:
        if isinstance(data, dict):
            for table in data.values():
                table.columns = table.columns.str.lower()
        else:
            data.columns = data.columns.str.lower()
    return data


def save_survey_with_pickle(obj, path):
    with open(path, 'wb') as output:
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)


def load_survey_from_pickle(path):
    with open(path, 'rb') as input:
        return pickle.load(input)


@copy_docstring(load_s3_data)
def load_s3_data_sv(profile_name=S3_PROFILE_NAME,
                    bucket_name=S3_BUCKET_NAME,
                    key=S3_KEY,
                    load_func=S3_LOAD_FUNC,
                    columnnames_to_lower=True
                    ):
    return load_s3_data(profile_name, bucket_name, key, load_func,
                        columnnames_to_lower=columnnames_to_lower)
