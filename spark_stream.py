import logging
import uuid
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

topic_name = 'voz'
kafka_server = 'localhost:9092'

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.comments (
        id UUID PRIMARY KEY,
        sentence TEXT
    );
    """)
    logging.info("Table created successfully with UUID as primary key!")

    print("Table created successfully!")


def insert_data(session, **kwargs):
    sentence = kwargs.get('sentence')
    try:
        session.execute("""
            INSERT INTO spark_streams.comments (id, sentence)
            VALUES (%s, %s)
        """, (uuid.uuid4(), sentence))
        logging.info(f"Data inserted for sentence: {sentence}")
    except Exception as e:
        logging.error(f'Could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', kafka_server) \
            .option('subscribe', topic_name) \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None



def create_selection_df_from_kafka(spark_df):
    if spark_df is None:
        logging.error("Input DataFrame is None, cannot proceed with transformation.")
        return None

    try:
        schema = StructType([
            StructField("sentence", StringType(), False)
        ])

        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
        logging.info("Transformation to selection DataFrame successful.")
        print(sel)
        return sel
    except Exception as e:
        logging.error(f"Error in transforming Kafka DataFrame: {e}")
        return None

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'comments')
                               .start())

            streaming_query.awaitTermination()

#  create success database