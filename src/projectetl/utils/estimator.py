from sklearn.model_selection import GridSearchCV
from sklearn.base import BaseEstimator
from sklearn.metrics import mean_squared_error

from criteriaetl.impute.model_based import get_train_test_indices
from criteriaetl.transformers.fusion_base import MergeTransformer


def get_estimator(estimator, X, y, grid_kwargs, df=None, weight=None, test_size=.2, seed=6202):
    train_index, test_index = get_train_test_indices(
        X if df is None else df, test_size, seed)

    if df is None:
        X_ = X.copy().loc[train_index].values
        y_ = y.copy().loc[train_index].values
        weight_ = weight.copy().loc[train_index].values if weight is not None else None
    else:
        X_ = df.loc[train_index, X].values
        y_ = df.loc[train_index, y].values
        weight_ = df.loc[train_index, weight].values if weight is not None else None

    grid_estimator = GridSearchCV(estimator, **grid_kwargs)
    grid_estimator = grid_estimator.fit(X_, y_, sample_weight=weight_)
    return grid_estimator, test_index


class ConditionalMeanEstimator(BaseEstimator):
    def __init__(self, groupby_features):
        self.features = groupby_features

    def fit(self, X, y, *, sample_weight=None, weight_col=None):
        self._df = X.copy()
        if sample_weight is None or weight_col is None:
            self._df['predicted_target_col'] = y.copy()
            self._df['predicted_target_col'] = self._df.groupby(
                self.features)['predicted_target_col'].transform('mean')
        else:
            self._df = self._df.merge(
                sample_weight, left_index=True, right_index=True)
            self._df['predicted_target_col'] = sample_weight \
                / self._df.groupby(self.features)[
                weight_col].transform('sum') * y
            self._df = self._df.groupby(self.features)[
                'predicted_target_col'].sum()
        return self

    def predict(self, X):
        _predicted = MergeTransformer(lambda: self._df,
                                      merge_kwargs={
                                          'on': self.features,
                                          'how': 'left'
                                      }).transform(X)
        return _predicted.fillna(0.0)['predicted_target_col']

    def score(self, y_true, y_pred, *, sample_weight=None):
        return mean_squared_error(y_true, y_pred, sample_weight=sample_weight)
