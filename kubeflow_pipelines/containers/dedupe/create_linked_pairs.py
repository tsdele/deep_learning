"""Module to create rules for manual linking and delinking pairs."""
import networkx

from pyspark.sql.functions import udf, col, unix_timestamp
from pyspark.sql.types import StringType, StructType, StructField
from py4j.protocol import Py4JJavaError

from pyspark_storage_tools import copy_json_to_bq
from create_duplicate_pairs import DedupeModelBuilder
import configs


class DedupeLinkBuilder(DedupeModelBuilder):
    """Extend the inherited class to include the linking and delinking logic."""

    def __init__(self, project_id, dataset_id, table_id,
                 manual_profile_dataset_id,
                 manual_profile_table_id):

        super(DedupeLinkBuilder, self).__init__(project_id, dataset_id,
                                                table_id)
        self.manual_profile_dataset_id = manual_profile_dataset_id
        self.manual_profile_table_id = manual_profile_table_id

    def link_delink_users(self):
        """Create rules based on user manual linking and delinking.

        :return: tuple of original clustering, delinking and linking rules
        :rtype: tuple
        """
        # Clustered pairs from dedupe process
        pairs_list_df = self.weighted_score()

        try:
            rdd_linked_users = self.ps_tools.get_bigquery_table_via_cloud(
                            self.ps_tools.spark_context, self.project_id,
                            self.manual_profile_dataset_id,
                            self.manual_profile_table_id
            )

            create_rdd_users_raw = (
                rdd_linked_users.map(lambda x: (
                                x['id_1'],
                                x['id_2'],
                                x['action'],
                                x['created_at']
                    )
                )
            )

            users_manual_df = (
                self.sql_context.createDataFrame(create_rdd_users_raw,
                                                ['id_1', 'id_2', 'action',
                                                 'created_at']))
        except Py4JJavaError:
            field = [StructField("id_1", StringType(), True),
                     StructField("id_2", StringType(), True),
                     StructField("action", StringType(), True),
                     StructField("created_at", StringType(), True)]
            schema = StructType(field)

            users_manual_df = self.sql_context.createDataFrame(
                    self.ps_tools.spark_context.emptyRDD(), schema)

        def main_de_linking_logic():
            """Create linking and delinking dataframes.

            :return: tuple of dataframes
            :rtype: tuple
            """
            # cast timestamp to datetime
            casted_df = (
                users_manual_df.select("*", unix_timestamp(
                            col("created_at")).cast("timestamp").alias("ts")))

            # Register temp table to query
            casted_df.registerTempTable("entries")

            # Create pyspark queries to filter out unique pairs with the
            # latest changes in date
            users_linked_df = self.sql_context.sql('''
                SELECT
                    *
                FROM (
                    SELECT
                        *,
                         dense_rank() OVER (PARTITION BY id_1, id_2 ORDER BY
                         ts DESC) AS rank
                    FROM entries
                ) vo WHERE rank = 1
            ''')

            # select those that have been delinked
            df_bool_linked_scores = users_linked_df.withColumn(
                    "bool_delinked", users_linked_df.action == 'delink')

            # filter those that are delinked
            filter_de_linked_scores_df = (
                df_bool_linked_scores.cache().filter(
                    col('bool_delinked') == True
                )
            )

            # select ids for those that are not linked
            filter_de_linked_scores = filter_de_linked_scores_df.select(
                                                            ['id_1', 'id_2'])

            #  filter those that are linked
            filter_linked_scores_df = (
                df_bool_linked_scores.cache().filter(
                    col('bool_delinked') == False
                )
            )

            filter_linked_scores = filter_linked_scores_df.selectExpr(
                                                "id_1 as record_fingerprint_1 ",
                                                "id_2 as record_fingerprint_2")

            return filter_de_linked_scores, filter_linked_scores

        filter_de_linked_scores, filter_linked_scores = main_de_linking_logic()

        return pairs_list_df, filter_de_linked_scores, filter_linked_scores

    def link_delink_users_rules(self):
        """Create rules for linking and delinking.

        :return: Union of pyspark Dataframe for user manual link/delink
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        (pairs_list_df,
         filter_de_linked_scores,
         filter_linked_scores) = self.link_delink_users()

        # Udf for linking and delinking
        udf_linked = udf(lambda x, y: 1 if x == y else 0)

        def first_delink_rule():
            """Apply the first delinking rule.

            :return: Pyspark dataframe of the record fingerprint
            :rtype: pyspark.sql.dataframe.DataFrame
            """
            first_id_cond = (
                (filter_de_linked_scores.id_1 ==
                 pairs_list_df.record_fingerprint_1)
            )

            first_id_cond_join = (
                filter_de_linked_scores.join(
                    pairs_list_df,
                    first_id_cond
                )
            )

            #
            first_linked_bool = first_id_cond_join.withColumn(
                "bool", udf_linked(
                        first_id_cond_join.id_2,
                        first_id_cond_join.record_fingerprint_2
                    ))

            # select from clustered pairs those that are linked given the
            # first condition
            first_delinked_bool = (
                first_linked_bool.cache().filter(
                    col('bool') == 0
                )
            )

            # select ids for for final clustering
            first_delinked_cluster = first_delinked_bool.select(
                ['record_fingerprint_1', 'record_fingerprint_2'])

            return first_delinked_cluster

        def second_delink_rule():
            """Apply the second delinking rule.

            :return: Pyspark dataframe of the record fingerprint
            :rtype: pyspark.sql.dataframe.DataFrame
            """
            second_id_cond = (
                (filter_de_linked_scores.id_1 ==
                 pairs_list_df.record_fingerprint_2)
            )

            second_id_cond_join = (
                filter_de_linked_scores.join(
                    pairs_list_df,
                    second_id_cond
                )
            )

            second_linked_bool = second_id_cond_join.withColumn(
                "bool", udf_linked(
                        second_id_cond_join.id_2,
                        second_id_cond_join.record_fingerprint_1
                    ))

            # select from clustered pairs those that are linked given the
            # second condition
            second_delinked_bool = (
                second_linked_bool.cache().filter(
                    col('bool') == 0
                )
            )

            # select ids for for final clustering
            second_delinked_cluster = second_delinked_bool.select(
                ['record_fingerprint_1', 'record_fingerprint_2'])

            return second_delinked_cluster

        def third_delink_rule():
            """Apply the third delinking rule.

            :return: Pyspark dataframe of the record fingerprint
            :rtype: pyspark.sql.dataframe.DataFrame
            """
            third_id_cond = (
                (filter_de_linked_scores.id_1 !=
                 pairs_list_df.record_fingerprint_1)
            )

            third_id_cond_join = (
                filter_de_linked_scores.join(
                    pairs_list_df,
                    third_id_cond
                )
            )

            third_linked_bool = third_id_cond_join.withColumn(
                "bool", udf_linked(
                        third_id_cond_join.id_2,
                        third_id_cond_join.record_fingerprint_2
                    ))

            # select from clustered pairs those that are linked given the
            # third condition
            third_delinked_bool = (
                third_linked_bool.cache().filter(
                    col('bool') == 0
                )
            )

            # select ids for for final clustering
            third_delinked_cluster = third_delinked_bool.select(
                ['record_fingerprint_1', 'record_fingerprint_2'])
            return third_delinked_cluster

        def fourth_delink_rule():
            """Apply the fourth delinking rule.

            :return: Pyspark dataframe of the record fingerprint
            :rtype: pyspark.sql.dataframe.DataFrame
            """
            fourth_id_cond = (
                (filter_de_linked_scores.id_1 !=
                 pairs_list_df.record_fingerprint_2)
            )

            fourth_id_cond_join = (
                filter_de_linked_scores.join(
                    pairs_list_df,
                    fourth_id_cond
                )
            )

            fourth_linked_bool = fourth_id_cond_join.withColumn(
                "bool", udf_linked(
                        fourth_id_cond_join.id_2,
                        fourth_id_cond_join.record_fingerprint_1
                    ))

            # select from clustered pairs those that are linked given the
            # fourth condition
            fourth_delinked_bool = (
                fourth_linked_bool.cache().filter(
                    col('bool') == 0
                )
            )

            # select ids for for final clustering
            fourth_delinked_cluster = fourth_delinked_bool.select(
                ['record_fingerprint_1', 'record_fingerprint_2'])

            return fourth_delinked_cluster

        def join_all_rules(link=True):
            """Combine all delinking rules.

            :param link Boolean to check if a manual link happened
            :type link: bool
            :return: Union of pyspark Dataframe
            :rtype: pyspark.sql.dataframe.DataFrame
            """
            if link:
                union_link_pair = self.union_all(
                    first_delink_rule(), second_delink_rule(),
                    third_delink_rule(), fourth_delink_rule(),
                    filter_linked_scores
                )
            else:
                union_link_pair = self.union_all(
                    first_delink_rule(), second_delink_rule(),
                    third_delink_rule(), fourth_delink_rule(),
                )
            return union_link_pair

        if (filter_de_linked_scores.rdd.isEmpty() and
                filter_linked_scores.rdd.isEmpty()):
            union_link_pair = pairs_list_df
        elif not filter_de_linked_scores.rdd.isEmpty():
            union_link_pair = join_all_rules(link=False)
        elif not filter_linked_scores.rdd.isEmpty():
            union_link_pair = self.union_all(
                filter_linked_scores, pairs_list_df
            )
        else:
            self.logger.info("Applying linking/delinking rules..")
            union_link_pair = join_all_rules()

        union_link_pair.show(10)

        union_link_pair_list = union_link_pair.rdd.map(
            lambda x: (x[0], x[1])).collect()

        return union_link_pair_list

    def process_pairs(self,
                      project_id,
                      target_dataset,
                      target_table):
        """Create clusters for duplicate pairs.

        :param project_id: project_name of project
        :type project_id: str
        :param target_dataset: location of the dataset for the outputs
        :type target_dataset: str
        :param target_table: location of the table for the outputs
        :type target_table: str
        """
        union_link_pair_list = self.link_delink_users_rules()

        self.logger.info("creating sets")
        graph_obj = networkx.Graph()
        graph_obj.add_edges_from(union_link_pair_list)

        def cluster_item(iter_num, user_set):
            """Create a json object that will be pushed to bigquery.

            :param iter_num: index number of iterator
            :type iter_num: int
            :param user_set: list of users in set
            :type user_set: list
            :return: json dict of each set of users
            :rtype: dict
            """
            return {
                'set': [{'user_id': s} for s in user_set],
                'cluster_id': str(iter_num).zfill(6)
            }

        clustered_sets_list = [
            cluster_item(iter_num, user_set)
            for iter_num, user_set in enumerate(
                    networkx.connected_components(graph_obj))
        ]

        copy_json_to_bq(
            clustered_sets_list, project_id, target_dataset, target_table
        )


def run_dedupe_model():
    """Run model."""
    model = DedupeLinkBuilder(
        project_id=configs.PROJECT_ID,
        dataset_id=configs.DATASET_ID,
        table_id=configs.TABLE_ID,
        manual_profile_dataset_id=configs.MANUAL_PROFILE_DATASET_ID,
        manual_profile_table_id=configs.MANUAL_PROFILE_TABLE_ID
    )

    model.process_pairs(
            project_id=configs.PROJECT_ID,
            target_dataset=configs.DATASET_ID,
            target_table=configs.TARGET_TABLE)

if __name__ == "__main__":
    run_dedupe_model()
