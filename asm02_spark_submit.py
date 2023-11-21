from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

#+---------------------------------+
# Khoi tao spark session & context
#+---------------------------------+

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('Assignment02') \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config('spark.mongodb.input.uri', 'mongodb://localhost/dep303_asm02') \
    .config('spark.mongodb.output.uri', 'mongodb://localhost/dep303_asm02') \
    .getOrCreate()

#+---------------------------------+
# 2. Đọc dữ liệu từ MongoDB với Spark
#+---------------------------------+

questions_schema = StructType([
    StructField("Id", IntegerType()),
    StructField("OwnerUserId", StringType()),
    StructField("CreationDate", StringType()),
    StructField("ClosedDate", StringType()),
    StructField("Score", IntegerType()),
    StructField("Title", StringType()),
    StructField("Body", StringType())
])

questions_raw = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .option("spark.mongodb.input.database", "dep303_asm01") \
    .option("collection", "Questions") \
    .schema(questions_schema) \
    .load()

#+---------------------------------+
answers_schema = StructType([
    StructField("Id", IntegerType()),
    StructField("OwnerUserId", StringType()),
    StructField("CreationDate", StringType()),
    StructField("ParentId", IntegerType()),
    StructField("Score", IntegerType()),
    StructField("Body", StringType())
])

answers_raw = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .option("spark.mongodb.input.database", "dep303_asm01") \
    .option("collection", "Answers") \
    .schema(answers_schema) \
    .load()

#+---------------------------------+
# 3. Chuẩn hóa dữ liệu
#+---------------------------------+

questions_df = questions_raw \
    .withColumn('CreationDate', col('CreationDate').cast(DateType())) \
    .withColumn('ClosedDate', col('ClosedDate').cast(DateType())) \
    .withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType())) \

answers_df = answers_raw \
    .withColumn('CreationDate', col('CreationDate').cast(DateType())) \
    .withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType())) \

#+---------------------------------+
# Dựa vào tập dữ liệu, hãy tính toán xem mỗi câu hỏi đang có bao nhiêu câu trả lời.
#+---------------------------------+

id_ques_df = questions_df \
    .select('Id') \
    .dropna()

id_ans_df = answers_df \
    .select(col('Id').alias('Id_ans'), 'ParentId') \
    .dropna()

# join 2 bang va tinh tong answers theo id question
cond_join = id_ques_df.Id == id_ans_df.ParentId

ques_have_ans_df = id_ques_df.join(id_ans_df, cond_join, 'inner') \
    .select('Id', 'Id_ans') \
    .groupBy('Id') \
    .agg(count('Id_ans').alias('Number of answers'))

ques_have_ans_df.show()

outputs_csv_path = '/home/vpnsaigon/airflow/data_asm2/outputs'

ques_have_ans_df.write \
    .format('csv') \
    .option('header', 'true') \
    .mode('overwrite') \
    .option('path', outputs_csv_path) \
    .save()

