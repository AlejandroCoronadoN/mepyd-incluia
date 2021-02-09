import pandas as pd

# point to covidsource
from covidsource.utils.dataload import load_survey_data, load_s3_data
from covidsource.utils.common_func import copy_docstring
from projectetl.utils.config import SURVEY_DATA_PATH, S3_PROFILE_NAME, \
    S3_BUCKET_NAME, S3_KEY, LOCAL_LOAD_FUNC, S3_LOAD_FUNC


@copy_docstring(load_survey_data)
def load_survey_data_sv(path=SURVEY_DATA_PATH,
                        load_func=LOCAL_LOAD_FUNC,
                        columnnames_to_lower=True,
                       ) -> pd.DataFrame:
    return load_survey_data(path, load_func,
                            columnnames_to_lower=columnnames_to_lower)


@copy_docstring(load_s3_data)
def load_s3_data_sv(profile_name=S3_PROFILE_NAME,
                    bucket_name=S3_BUCKET_NAME,
                    key=S3_KEY,
                    load_func=S3_LOAD_FUNC,
                    columnnames_to_lower=True
                    ):
    return load_s3_data(profile_name, bucket_name, key, load_func,
                           columnnames_to_lower=columnnames_to_lower)