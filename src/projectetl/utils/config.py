from pathlib import Path
import pandas as pd
import io

# loading configurations
data_dir = (Path(__file__).parent.parent.parent.parent / "data").resolve()
SURVEY_DATA_PATH = (Path(data_dir) / "survey" / "EHPM 2019.sav").resolve()
S3_PROFILE_NAME = 'rodrigo'
S3_BUCKET_NAME = 'covid-response-sv'
S3_KEY = 'isss/ISSS_19.xlsx'


def LOCAL_LOAD_FUNC(path): return pd.read_spss(
    str(path), convert_categoricals=False)


def S3_LOAD_FUNC(obj): return pd.read_excel(
    io.BytesIO(obj['Body'].read()), encoding='utf8')


# Global variables
key_variable_isss = 'nit'
key_variable_ehpm = 'idboleta'
