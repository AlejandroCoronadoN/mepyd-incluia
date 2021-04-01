import pandas as pd


def get_weighted_average(df, col_to_average, weight_col, groupby=None):
    '''
    Computes the weighted average of pandas column using weitghts from
    another column

    Parameters
    ----------
    df : DataFrame,
        where columns are stored.
    col_to_average : hashable,
        points to the column from which the average is computed
    weight_col : hashable,
        point to the column where weights are extracted
    groupby : column label or iterable of column labels
        If not None, compute the weighted average by groups.
    '''
    average_df = df[[col_to_average, weight_col]].copy()
    average_df.loc[:, '_expanded'] = (average_df[col_to_average].mul(df[weight_col]))
    
    if groupby is None:
        unpacked_division = average_df['_expanded'] / average_df[weight_col].sum()
        return unpacked_division.sum()
    
    else:
        grouped_average_df = pd.concat((average_df, df[groupby].to_frame()), axis=1)
        grouped_average_df = grouped_average_df.groupby(groupby)[['_expanded', weight_col]].sum()
        grouped_average_df.loc[:, col_to_average] = grouped_average_df._expanded.div(
            grouped_average_df[weight_col])
        return grouped_average_df[[col_to_average, weight_col]]
