from criteriaetl.impute.model_based import get_train_test_indices
from sklearn.model_selection import GridSearchCV


def get_estimator(estimator, X, y, grid_kwargs, df=None, weight=None, test_size=.2, seed=6202):
    test_index = None

    if df is None:
        X_ = X.copy()
        y_ = y.copy()
        weight_ = weight.copy()
    else:
        test_index, train_index = get_train_test_indices(df, test_size, seed)
        X_ = df.loc[train_index, X].values
        y_ = df.loc[train_index, y].values
        weight_ = df.loc[train_index, weight].values if weight else None

    grid_estimator = GridSearchCV(estimator, **grid_kwargs)
    grid_estimator = grid_estimator.fit(X_, y_, sample_weight=weight_)
    return grid_estimator, test_index
