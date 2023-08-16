from pyspark.sql import DataFrame
from pyspark.sql.functions import *

#filler age 
def filler_col(df: DataFrame) -> DataFrame:
    
    return df.filter((df["Customer_Age"] >= 25) & (df["Customer_Age"] <= 50) & (df["Education_Level"] != "Unknown"))

#select 
def select_col(df: DataFrame):

    return df.select("Customer_Age","Education_Level","Income_Category","Months_on_book")

