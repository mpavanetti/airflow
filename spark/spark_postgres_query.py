from airflow.models import Variable
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark_dir = Variable.get("spark_dir")
    
    # Start Spark Session
    spark = (SparkSession 
                .builder  
                .master("local[1]")
                .appName("postgres_query")
                .getOrCreate())
    
    pg_read = (spark.read
                    .format("jdbc")
                    .option("url", "jdbc:postgresql://postgres:5432/airflow")
                    .option("user", "airflow")
                    .option("password", "airflow")
                    .option("driver", "org.postgresql.Driver"))
    
    dataset_df = (pg_read
                  .option("query", "SELECT * FROM VW_DATASET_1")
                  .load())
    
    dataset_df.show(15,False)
    dataset_df.printSchema()