import numpy as np


def get_real_feature_names(df, features):
    return np.array([scol for fn in features for scol in df if scol.startswith(fn)])


def split_survey_by(survey, column_name, values):
    return tuple(survey[survey[column_name] == value] for value in values)
