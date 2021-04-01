def inspect_nulls(df):
    """Returns the number of nulls on all columns ehich present at least
     one null"""
    nulls_inspect = df.isnull().sum()
    return nulls_inspect[nulls_inspect > 0]