from pathlib import Path
import pandas as pd
import io

# loading configurations
data_dir = (Path(__file__).parent.parent.parent.parent / "data").resolve()
SURVEY_DATA_PATH = (Path(data_dir) / "survey" / "EHPM 2019.sav").resolve()

S3_BUCKET_NAME = 'do-siuben'
S3_KEY_DICT = {
    'miembros': 'parsed/miembros_v3.pk.gz',
    'miembros_v2': 'parsed/miembros_v2.pk.gz',
    'miembros_v1': 'parsed/miembros.pk.gz',
    'hogares': 'parsed/hogares_v2.pk.gz',
    'hogares_v1': 'parsed/hogares.pk.gz',
    'hogares_raw': 'raw/SIUBEN_hogares.csv.gz',
    'hogares_complement_v2_raw': 'raw/SIUBEN-SUPERATE.csv.gz',
    'miembros_raw': 'raw/SIUBEN_miembros.csv.gz',
    'miembros_complement_v2_raw': 'raw/Variables_faltantes_2.csv.gz',
    'miembros_complement_v3_raw': 'raw/SIUBEN cond laboral.csv.gz',
}
S3_SUBPRODUCTS_DIR = ''


def LOCAL_LOAD_FUNC(path): return pd.read_spss(
    str(path), convert_categoricals=False)


def S3_LOAD_FUNCTIONAL(s3_key, **kwargs):
    if s3_key.endswith(('.pk.gz', '.p.gz')):
        return lambda obj: pd.read_pickle(
        io.BytesIO(obj['Body'].read()), compression='gzip', **kwargs)
    if s3_key.endswith(('.csv', '.csv.gz')):
        return lambda obj: pd.read_csv(
            io.BytesIO(obj['Body'].read()), **kwargs)


# Global variables
key_variable_isss = 'nit'
key_variable_ehpm = 'idboleta'
