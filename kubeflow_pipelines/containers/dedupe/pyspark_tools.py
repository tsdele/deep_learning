"""Pyspark functions that get used repeatedly."""
import json
import time

import pyspark
from pyspark.sql.types import StructField, StructType, StringType

from modcloud import storage


class PysparkTools(object):
    """Common tools used in pyspark projects."""

    def __init__(self):

        self.spark_context = self.instantiate_pyspark()
        log4j_logger = self.spark_context._jvm.org.apache.log4j
        self.logger = log4j_logger.LogManager.getLogger(__name__)

    @staticmethod
    def instantiate_pyspark():
        """Instantiate pyspark for online use.
        :return: pyspark SparkContext object
        :rtype: pyspark.SparkContext
        """
        spark_context = pyspark.SparkContext()
        return spark_context

    def get_storage_table_via_cloud(self, gcs_path):
        """Get a table from google cloud storage.

        :param gcs_path: google storage path
        :type gcs_path: str
        :return: tuple containing schema and the rdd dataset
        :rtype: tuple
        """

        rdd_all_users = self.spark_context.textFile(gcs_path)
        header = rdd_all_users.first()  # returns a string
        schema_string = header.replace('"', '')  # get rid of the double-quotes
        fields = [StructField(field_name, StringType(), True)
                  for field_name in
                  schema_string.split(',')]

        schema = StructType(fields)

        self.logger.info(header)
        rdd_header = self.spark_context.parallelize([header])
        rdd_all_users = rdd_all_users.subtract(rdd_header)

        # split lines for a row
        rdd_all_users_split = rdd_all_users.map(lambda line: line.split(","))

        return schema, rdd_all_users_split

    @staticmethod
    def get_bigquery_table_via_cloud(spark_context,
                                     project_id,
                                     dataset_id,
                                     table_id,
                                     nr_partitions=None):
        """Get a table from bigquery.
        :param spark_context: spark context object
        :type spark_context: SparkContext
        :param project_id: google project_id
        :type project_id: str
        :param dataset_id: bigquery source table dataset id
        :type dataset_id: str
        :param table_id: bigquery source table id
        :type table_id: str
        :param nr_partitions: number of partitions that the rdd is split into.
          Should be 2x number of nodes.
        :type nr_partitions: int
        :return: rdd loaded from file
        :rtype: RDD
        """
        hadoop_conf = spark_context._jsc.hadoopConfiguration()
        project = hadoop_conf.get('fs.gs.project.id')
        bucket = hadoop_conf.get("fs.gs.system.bucket")

        conf = {
            "mapred.bq.project.id": project,
            "mapred.bq.gcs.bucket": bucket,
            "mapred.bq.input.project.id": project_id,
            "mapred.bq.input.dataset.id": dataset_id,
            "mapred.bq.input.table.id": table_id
        }

        rdd_dataset_raw = spark_context.newAPIHadoopRDD(
            'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
            'org.apache.hadoop.io.LongWritable',
            'com.google.gson.JsonObject',
            conf=conf
        )

        # repartition if paritions defined
        if not nr_partitions:
            rdd_dataset = (
                rdd_dataset_raw
                .map(lambda t: json.loads(t[1]))
            )
        else:
            rdd_dataset = (
                rdd_dataset_raw
                .partitionBy(nr_partitions)
                .map(lambda t: json.loads(t[1]))
            )
        return rdd_dataset

    def save_to_gs(self, rdd, bucket, name):
        """Save the rdd to google storage.
        :param rdd: Pyspark RDD that needs to get saved
        :type rdd: pyspark.RDD
        :param bucket: name of bucket where data should be stored
        :type bucket: str
        :param name: path after bucket where data should be stored
        :type name: str
        :return: None
        :rtype: None
        """
        o_storage = storage.Storage()

        gs_path = "gs://{}/{}".format(bucket, name)

        # remove previous dir if exists
        started_remove = False
        while o_storage.list_bucket(bucket, name):
            # if there is something in the bucket name
            if not started_remove:
                self.logger.info('removing {}/{}.'.format(bucket, name))
                o_storage.clear_bucket(bucket, name)
                started_remove = True
            else:
                self.logger.info(
                    'not all files yet removed from {}. '
                    ' Waiting 5 sec...'.format(gs_path)
                )
                time.sleep(5)

        self.logger.info("saving to google storage: {}".format(gs_path))
        rdd.saveAsTextFile(gs_path)
