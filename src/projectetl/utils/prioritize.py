def normalize_prioritization_variable(
        X, continuous_feature, threshold_col, n_buckets_threshold,
        clip_bounds=None
):
    """
    Normalize continuous feature proportional to a selected threshold.

    Parameters
    ----------
    X : pd.DataFrame,
        Where columns are extracted.
    continuous_feature : column label,
        Indicates which column will be binarized.
    threshold_col : column label,
        Indicates which column is used as threshold.
    n_buckets_threshold : int,
        number of bins to be used to reach threshold.
    clip_bounds: tuple (lower, upper) or None
        Set clip bounds (lower, upper) on pd.Series.clip. It collapses
        all values which are beyond lower and upper bounds.
    """

    normalize_srs = X[continuous_feature].div(X[threshold_col]).mul(
        n_buckets_threshold)

    if clip_bounds is not None:
        lower, upper = clip_bounds
        normalize_srs = normalize_srs.clip(lower, upper)

    return normalize_srs