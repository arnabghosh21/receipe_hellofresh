from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import when, col, regexp_replace, regexp_extract, avg
from pyspark.sql.types import IntegerType
import logging

spark = SparkSession.builder.appName("sparkTest1").getOrCreate()

def getProcessedData(filepath):
    try:
        df = spark.read.format("json").option("header", "true").load(filepath)
    #dropping unnecessary columns, selecting only 3 columns
        df = df.select(['cookTime', 'ingredients', 'prepTime'])
    #removing prefix 'PT' in both columns
        df = df.withColumn("cookTime", regexp_replace(col("cookTime"), "^[^0-9]+", ""))
        df = df.withColumn("prepTime", regexp_replace(col("prepTime"), "^[^0-9]+", ""))
    #separating hours & minutes in separate columns
        df = df.withColumn("CThours", regexp_extract("cookTime", "(\d+)H", 1))
        df = df.withColumn("CTminutes", regexp_extract("cookTime", "(\d+)M", 1))
        df = df.withColumn("PThours", regexp_extract("prepTime", "(\d+)H", 1))
        df = df.withColumn("PTminutes", regexp_extract("prepTime", "(\d+)M", 1))
    #changing dtypes of hours & minutes columns to int
        df = df.withColumn("CThours", df.CThours.cast(IntegerType()))
        df = df.withColumn("CTminutes", df.CTminutes.cast(IntegerType()))
        df = df.withColumn("PThours", df.PThours.cast(IntegerType()))
        df = df.withColumn("PTminutes", df.PTminutes.cast(IntegerType()))
    #replacing null values with 0
        df = df.fillna(0, subset=["CThours"])
        df = df.fillna(0, subset=["CTminutes"])
        df = df.fillna(0, subset=["PThours"])
        df = df.fillna(0, subset=["PTminutes"])
    #calculating total duration in minutes
        df = df.withColumn("total_ct_minutes", col("CThours") * 60 + col("CTminutes"))
        df = df.withColumn("total_pt_minutes", col("PThours") * 60 + col("PTminutes"))
    #calculating total_cook_time
        df = df.withColumn("total_cook_time", col("total_ct_minutes") + col("total_pt_minutes"))
    #removing zeros
    #all blank records got deleted
    #one record, there is PT in both columns. so, that is also got eleminated
    #first we add both durations, then removed zeros
        df = df.filter(df["total_cook_time"] != 0)
    #selecting two columns to be displayed
        df = df.select(['ingredients', 'total_cook_time'])
        return df

    except Exception as e:
        logging.error("An error occurred while processing the data: %s", e)
        raise e
#below, we will merge all 3 given files & make a single dataframe
def getMerged():
    try:
        df000 = getProcessedData(r'..\input\recipes-000.json')
        df001 = getProcessedData(r'..\input\recipes-001.json')
        df002 = getProcessedData(r'..\input\recipes-002.json')
        merged_df = df000.union(df001)
        merged_df2 = merged_df.union(df002)
        return merged_df2
    except Exception as e:
        logging.error("Error in getMerged: " + str(e))
        raise

#as per Task2 point1, we will extract only receipes, which contains 'beef'
def findBeef():
    try:
        dfFindBeef = getMerged().withColumn("ingredients", func.lower(col("ingredients")))
        dfFindBeef = dfFindBeef.filter(col("ingredients").contains("beef"))
        return dfFindBeef
    except Exception as e:
        logging.error("Error in findBeef: " + str(e))
        raise
#as per Task2 point2, we will find out the difficulty level & average cooking time per difficulty level
def findAvgCookingTime():
    try:
        df2 = findBeef()
        dfFindDifficulty = df2.withColumn("difficulty", when(df2["total_cook_time"] < 30, "easy").when((df2["total_cook_time"] >= 30) & (df2["total_cook_time"] <= 60), "medium").otherwise("hard"))
        dfFindDifficulty = dfFindDifficulty.select(['total_cook_time', 'difficulty']).sort('difficulty')
        dfFindAvgCookingTime = dfFindDifficulty.groupBy("difficulty").agg(avg("total_cook_time").alias("avg_total_cooking_time"))
        dfFindAvgCookingTime = dfFindAvgCookingTime.select(["difficulty", "avg_total_cooking_time"])
        return dfFindAvgCookingTime
    except Exception as e:
        logging.error("Error in findAvgCookingTime: " + str(e))
        raise
#as per task2, output to csv
def writeCsv(outputFileName):
    try:
        findAvgCookingTime().write.mode('overwrite').csv(outputFileName, header=True)
    except Exception as e:
        logging.error("Error in saving findAvgCookingTime: " + str(e))
        raise
#calling writeCsv function
writeCsv('OutputCSV')