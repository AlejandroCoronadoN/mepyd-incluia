from pathlib import Path
import pandas as pd
import io

# data loading configuration
DATA_DIR = (Path(__file__).parent.parent.parent.parent / "data").resolve()

# encft
ENCFT_DIR = (Path(DATA_DIR) / "survey" / "ENCFT").resolve()
ENCFT_SURVEY_PATH = (Path(ENCFT_DIR) / "Base ENCFT 20201 - 20204" /
                     "Base ENCFT 20201 - 20204 - hogar.xlsx").resolve()
ENCFT_PREVIOUS_SURVEY_PATH = (
    Path(ENCFT_DIR) / "BD ENCFT 20161-20202.sav").resolve()
ENCFT_OBJECT_DIR = (Path(ENCFT_DIR) / "object").resolve()

# inflation
INFLATION_DIR = (Path(DATA_DIR) / 'inflation').resolve()
INFLATION_OBJECT_DIR = (Path(INFLATION_DIR) / 'object')

# enhogar
ENHOGAR_DIR = (Path(DATA_DIR) / 'survey' / 'ENHOGAR').resolve()

# S3 profile
S3_PROFILE_NAME = 'rodrigo'
S3_BUCKET_NAME = 'covid-response-sv'
S3_KEY = 'isss/ISSS_19.xlsx'


def LOCAL_LOAD_FUNC(path):
    return pd.read_excel(str(path), sheet_name=None)


def S3_LOAD_FUNC(obj): return pd.read_excel(
    io.BytesIO(obj['Body'].read()), encoding='utf8')
