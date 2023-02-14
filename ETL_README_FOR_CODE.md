# Introduction

Here we will deal with some recipe data. The task is to process the data, handle missing values, filter recipes with 'beef' as ingredients & find out their average cook time, according to their difficulty level.
### Approach
We have 9 columns in each given file. But do not require all the columns. We only require 3 columns: 'ingredients', 'cookTime' & 'prepTime'. We will follow below approach:
1. At first, we will filter out other columns & take only those 3 columns.
2. With some operations, we will add 'cookTime' & 'prepTime' column & create a new column, 'total_cook_time' with total duration(cookTime + prepTime).
3. Now we will filter out records with total_cook_time = 0. So, here the approach is: if either cookTime or prepTime is having a value > 0, then we will consider that record. Otherwise, we will remove.
4. We will filter only 'ingredients' & 'total_cook_time' columns and use those in next steps.
5. Now it's time to merge all three input files(3 dfs into 1 df).
6. Next, will find out the ingredients with 'beef'
7. Now we find out difficulty level according to given condition.
8. And we will calculate the 'avg_total_cooking_time' for each difficulty level.
9. At the last we will save the output in csv format.

### Prerequisites

Before running this code, the following must be installed on the system:
* •	PySpark
* •	Python 3.x
* •	Java 8 or higher

### Initial Setup

The following code imports the necessary libraries and creates a Spark session.

`from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import when, col, regexp_replace, regexp_extract, avg
from pyspark.sql.types import IntegerType
import logging
spark = SparkSession.builder.appName("sparkTest1").getOrCreate() `

### getProcessedData(outputFileName)

This function takes in a file path ref1 and performs the following operations:
1.	Reads the recipe data from the file using PySpark's read method and selects relevant columns 'cookTime', 'ingredients', and 'prepTime'.
2.	Replaces all non-numeric characters in columns 'cookTime' and 'prepTime' with an empty string using the regexp_replace function.
3.	Extracts the number of hours and minutes for cooking time and preparation time using regexp_extract and creates new columns 'CThours', 'CTminutes', 'PThours', and 'PTminutes'.
4.	Converts columns 'CThours', 'CTminutes', 'PThours', and 'PTminutes' to integer data type using cast method.
5.	Fills in any missing values in columns 'CThours', 'CTminutes', 'PThours', and 'PTminutes' with 0 using fillna method.
6.	Calculates the total cooking time and preparation time in minutes and creates new columns 'total_ct_minutes', 'total_pt_minutes', and 'total_cook_time'.
7.	Filters out the rows where 'total_cook_time' is 0. Here we are removing the duration with 0 value.
8.	Selects only the 'ingredients' and 'total_cook_time' columns.

### getMerged()

This function calls the getProcessedData function multiple times to read data from three different files and merges the data into a single dataframe. The data from recipes-000.json, recipes-001.json, and recipes-002.json is merged using PySpark's union method.

### findBeef()
This function calls getMerged and performs the following operations:
1.	Converts all values in the 'ingredients' column to lowercase using the lower function.
2.	Filters out all rows where the 'ingredients' column does not contain the word 'beef'.

### findAvgCookingTime()
This function calls findAvgCookingTime and performs the following operations:
1. The function first calls the findBeef() function (whose implementation is not shown) to retrieve the data on beef dishes.
2. The data is then processed using Spark SQL functions:
3. withColumn is used to create a new column "difficulty" based on the value of "total_cook_time".
4. when is used to assign the difficulty level to each dish (easy, medium, hard).
5. sort is used to sort the data by the difficulty level.
6. groupBy and agg are used to group the data by difficulty level and calculate the average cooking time of each group.
7. select is used to select the desired columns from the processed data.
8. The processed data is returned as a dataframe.
9. In the next section, the processed data is written to a CSV file using write and mode('overwrite')

### writeCsv(outputFileName)
This function writeCsv performs the following operations:
1.	The function writes the result of findAvgCookingTime method to a CSV file.
2.	writeCsv takes one argument outputFileName, which is the name of the output CSV file.
3.	It uses the write method of the Spark DataFrame API to write the result of the findAvgCookingTime method to a CSV file with mode='overwrite' and header=True.
4.	The function is called by passing 'findAvgCookingTimeForBeefData' as an argument to the writeCsv method.

### Error Handling

In case of an error, the functions logs the error message using the logging module and raises an exception. The same error handling mechanism applies to the saving process of the data.

### Bonus points

##### Config management.
Here we let the spark decide to allocate executor memory & driver memory as we do not know what is the maximum data size we can expect. We can easily specify these things using .config()

##### Data quality checks (like input/output dataset validation).
Here we have performed some operations to validate the data.
* Used 'fillna' to replace null values with 0
* Used 'filter' to handle missing values
* Used 'cast' to change the datatype

##### How would you implement CI/CD for this application?
We can use several tools like: Git, Jenkins, Kubernetes etc. to implement ci/cd for this application. We can also use cloud services like: Azure, AWS, GCP.
##### How would you diagnose and tune the application in case of performance problems?
PySpark provides functionality to diagnose performance issues such as Spark UI, Spark logs, and Spark configuration parameters. We can use re-partitioning & lazy evaluation. Instead of using csv, we can use parquet format. 
##### How would you schedule this pipeline to run periodically?
We can schedule this pipeline using Azure Data Factory (ADF). To schedule the pipeline, we can use Databricks notebooks to add teh code to the pipeline, and then schedule the pipeline to run on a specified schedule using a trigger. We can set the trigger to run on a recurring basis(periodically), such as daily, weekly, or monthly. In this way, we can automate the process of running this pipeline and ensure that it is run on a regular schedule.

##### We appreciate a combination of Software and Data Engineering good practices.
We have used the best practices to write the code.

### Conclusion

This code provides a solution for processing and analyzing the data given in JSON format. It demonstrates how to use Spark's read method, how to process and manipulate data using Spark DataFrames, and how to handle errors using logging.



###### ------------------------------------------------------------Thank you------------------------------------------------------------
