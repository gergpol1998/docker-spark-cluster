from pyspark.sql import DataFrame


def rename_cols(df: DataFrame, mapping_dict: dict) ->DataFrame:
    # Rename all the columns
    '''
    :param df: input dataframe
    :param mapping_dict: dict of columns names
    :return: ouput dataframe
    '''
    for key in mapping_dict.keys():
        df = df.withColumnRenamed(key,mapping_dict.get(key))
    return df


def specific_cols(df: DataFrame, specific_cols: list):
    # get specific cols df
    '''
    :param df: input dataframe 
    :param specific_cols: list of columns names
    :return: ouput dataframe
    '''
    return df.select(specific_cols)



def join_df(left_df: DataFrame, right_df: DataFrame, ON_COLUMNS:list, JOIN_TYPE: str)->DataFrame:
    # Join two dataframes
    '''
    :param left_df: input dataframe
    :param right_df: input dataframe
    :param ON_COLUMNS: list of columns to perform join
    :param JOIN_TYPE: Join type
    :return: ouput dataframe
    '''
    output_df = left_df.alias("left_df").join(right_df.alias("right_df"), ON_COLUMNS, JOIN_TYPE)
    return output_df