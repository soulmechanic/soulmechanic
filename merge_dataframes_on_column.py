import pandas as pd

def merge_dataframes_on_column(dfs, column):
    """
    Merges a list of dataframes on a specified column.

    :param dfs: A list of dataframes to merge
    :type dfs: list
    :param column: The column to merge on
    :type column: str
    :return: The merged dataframe or None if an error occurred
    :rtype: pd.DataFrame or None
    """
    # Check for empty dataframes or dataframes that are not of type DataFrame
    empty_df_index = next((i for i, df in enumerate(dfs) if not isinstance(df, pd.DataFrame) or df.empty), None)
    if empty_df_index is not None:
        print(f"Error: Dataframe at index {empty_df_index} is empty or not of type DataFrame")
        return None

    try:
        # Merge the dataframes on the specified column
        result = dfs[0]
        for df in dfs[1:]:
            result = result.merge(df, on=column)
        return result
    except Exception as e:
        # Handle any exceptions that may occur during the merging process
        print(f"An error occurred: {e}")
        return None
