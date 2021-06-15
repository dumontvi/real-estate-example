def _pd_to_csv(self, df):
    """
    Pandas DF to csv buffer for write to s3
    """
    csv_buffer = StringIO()
    return df.to_csv(csv_buffer)


def _flatten(df, cols):
    """
    Iteratively flatten the unstructured dataframe cols
    """
    pass
