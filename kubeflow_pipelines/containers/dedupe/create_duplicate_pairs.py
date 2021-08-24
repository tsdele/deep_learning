"""Module to create duplication pairs."""
import re
from functools import reduce
from fuzzywuzzy import fuzz

from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType

import configs
from pyspark_tools import PysparkTools


class DedupeModelBuilder(object):
    """Class that creates deduplicate pair of records ...
    """

    def __init__(self, project_id, dataset_id, table_id):

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.ps_tools = PysparkTools()
        self.logger = self.ps_tools.logger
        self.gen_var = GenerateVariable()
        self.sql_context = SQLContext(self.ps_tools.spark_context)

    def create_data_frame(self):
        """Create dataframe.
        :return: dataframe and its duplicate copy
          dataframe is of type pyspark.sql.dataframe.DataFrame
        :rtype: tuple
        """
        self.logger.info("fetching and preprocessing data...")

        rdd_all_users = self.ps_tools.get_bigquery_table_via_cloud(
                self.ps_tools.spark_context, self.project_id, self.dataset_id,
                self.table_id
        )

        create_rdd_data_raw = (
            rdd_all_users.map(lambda x, strip=self.strip_whitespace: (
                            strip(x['full_name']),
                            strip(x['first_name']),
                            strip(x['last_name']),
                            x['email_address'].lower(),
                            x['home_telephone_number'],
                            x['work_telephone_number'],
                            x['mobile_telephone_number'],
                            x['sourced_from'],
                            x['organization_id'],
                            x['record_fingerprint']
                )
            )
        )

        # call a dataframe context
        users_dedupe_df = self.sql_context.createDataFrame(create_rdd_data_raw,
                                               configs.DEDUPE_CONFIGS_COLUMNS)

        # Create a unique id in the dataframe
        users_id_df = users_dedupe_df.rdd.zipWithIndex().map(
                lambda x: [x[1]] + [y for y in x[0]]).toDF(
                ['id']+users_dedupe_df.columns)

        # get the the dedupe columns
        dedupe_columns = users_id_df.columns
        self.logger.info(dedupe_columns)

        # create a duplicate dataframe and change column names
        duplicate_df = (users_id_df.alias("duplicate_df"))

        replacements_name_dict = {
            c: self.gen_var.create_index_variable(1, c)
            for c in dedupe_columns
        }
        original_df_expr = users_id_df.select(
            [col(c).alias(replacements_name_dict.get(c, c))
             for c in users_id_df.columns]
        )
        replacements_name_dict = {
            c: self.gen_var.create_index_variable(2, c)
            for c in dedupe_columns
        }
        duplicate_df_expr = duplicate_df.select(
            [col(c).alias(replacements_name_dict.get(c, c))
             for c in duplicate_df.columns]
        )
        # keep in-memory
        duplicate_df_expr = duplicate_df_expr.persist()
        return original_df_expr, duplicate_df_expr

    def create_dedupe_rules(self):
        """Create joining rules.
        :return: union of all join rules
        :rtype: pyspark.DataFrame
        """
        self.logger.info("read data frame...")
        original_df_expr, duplicate_df_expr = self.create_data_frame()
        self.logger.info("creating dedupe rules..")

        last_name_cond = (
            (original_df_expr.id_1 < duplicate_df_expr.id_2) &
            (original_df_expr.last_name_1 == duplicate_df_expr.last_name_2) &
            (original_df_expr.first_name_1 == duplicate_df_expr.first_name_2)
        )

        last_name_cond_join = (
            original_df_expr.join(
                duplicate_df_expr,
                last_name_cond
            )
        )

        fullname_cond = (
            (original_df_expr.id_1 < duplicate_df_expr.id_2) &
            (original_df_expr.sourced_from_1 !=
             duplicate_df_expr.sourced_from_2) &
            (original_df_expr.full_name_1
             == duplicate_df_expr.full_name_2)
        )

        fullname_cond_join = (
            original_df_expr.join(
                duplicate_df_expr,
                fullname_cond
            )
        )

        name_surname_cond = (
            (original_df_expr.id_1 < duplicate_df_expr.id_2) &
            (original_df_expr.first_name_1 ==
             duplicate_df_expr.full_name_2)
        )

        name_surname_cond_join = (
            original_df_expr.join(
                duplicate_df_expr,
                name_surname_cond
            )
        )

        email_cond = (
            (original_df_expr.id_1 < duplicate_df_expr.id_2) &
            (original_df_expr.email_address_1 ==
             duplicate_df_expr.email_address_2)
        )

        email_cond_join = (
            original_df_expr.join(
                duplicate_df_expr,
                email_cond
            )
        )

        phone_cond = (
            (original_df_expr.id_1 < duplicate_df_expr.id_2) &
            (original_df_expr.mobile_telephone_number_1 ==
             duplicate_df_expr.mobile_telephone_number_2)
        )

        phone_cond_join = (
            original_df_expr.join(
                duplicate_df_expr,
                phone_cond
            )
        )

        self.logger.info("joining all rules.")
        union_filtered_pair = self.union_all(
            last_name_cond_join, fullname_cond_join, name_surname_cond_join,
            email_cond_join, phone_cond_join
        )
        union_filtered_pair.show(10)
        return union_filtered_pair

    def calculate_dedupe_scores(self):
        """Calculate Deduplication scores for product names and brands.
        :return: Dataframe with scores for pairs of product name and brands
        :rtype:pyspark.sql.dataframe.DataFrame
        """

        union_filtered_pair = self.create_dedupe_rules()

        string_distance_udf = udf(
            lambda x, y: 0.0 if len(x) == 0 and len(y) == 0 else
            (0.0 if fuzz.ratio(x, y) / 100.0 < 0.76 else
             fuzz.ratio(x, y) / 100.0), FloatType()
        )

        dedude_scores_df = (
            union_filtered_pair
            .withColumn(
                "full_name_ratio",
                string_distance_udf(
                    union_filtered_pair.full_name_1,
                    union_filtered_pair.full_name_2
                )
            )
            .withColumn(
                "last_name_ratio",
                string_distance_udf(
                    union_filtered_pair.last_name_1,
                    union_filtered_pair.last_name_2
                )
            )
            .withColumn(
                "email_address_ratio",
                string_distance_udf(
                    union_filtered_pair.email_address_1,
                    union_filtered_pair.email_address_2
                )
            )
            .withColumn(
                "first_name_ratio",
                string_distance_udf(
                    union_filtered_pair.first_name_1,
                    union_filtered_pair.first_name_2
                )
            )
            .withColumn(
                "mobile_telephone_number_ratio",
                string_distance_udf(
                    union_filtered_pair.mobile_telephone_number_1,
                    union_filtered_pair.mobile_telephone_number_2
                )
            )
        )

        # # udf that checks if full_name  score is greater than 0.5
        udf_scores = udf(lambda y: 1 if y > 0.5 else 0)
        dedude_user_name_scores_df = dedude_scores_df.withColumn(
            "bool", udf_scores(
                    dedude_scores_df.full_name_ratio
                ))
        #
        filter_full_name_scores = \
            dedude_user_name_scores_df.cache().filter(col('bool') == 1)

        filter_full_name_scores.show(50)

        return filter_full_name_scores

    def weighted_score(self,
                       w_full_name=0.4,
                       w_last_name=0.3,
                       w_first_name=0.1,
                       w_email=0.1,
                       w_phone=0.1):
        """Calculate the weighted score for a dedupe configs.
        :param w_full_name: weight for the full name
        :type w_full_name: float
        :param w_last_name: weight for the last name
        :type w_last_name: float
        :param w_first_name: weight for the first name
        :type w_first_name: float
        :param w_email: weight for the email address
        :type w_email: float
        :param w_phone: weight for the phone number
        :type w_phone: float
        :return: list of potential pairs
        :rtype: list
        """
        filter_full_name_scores = self.calculate_dedupe_scores()
        weighted_score_udf = udf(
            lambda a, b, c, d, e:
            w_full_name * a + w_last_name * b
            + w_first_name*c + w_email*d + w_phone*e)

        # calculate the weighted scores
        df_weighted_scores = (
            filter_full_name_scores.withColumn(
                "weighted_scores",
                weighted_score_udf(
                    filter_full_name_scores.full_name_ratio,
                    filter_full_name_scores.last_name_ratio,
                    filter_full_name_scores.first_name_ratio,
                    filter_full_name_scores.email_address_ratio,
                    filter_full_name_scores.mobile_telephone_number_ratio
                )
            )
        )

        df_bool_scores = df_weighted_scores.withColumn(
                "bool_scores", df_weighted_scores.weighted_scores >= 0.4)

        filter_bool_score = (
            df_bool_scores.cache().filter(
                col('bool_scores') == True
            )
        )
        pairs_list_df = filter_bool_score.select(
            ['record_fingerprint_1', 'record_fingerprint_2'])

        return pairs_list_df

    @staticmethod
    def union_all(*dfs):
        """Function that take the union of all dataframes.
        :param dfs:  dataframes
        :return: Union of all dataframes
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        return reduce(DataFrame.unionAll, dfs)

    @staticmethod
    def strip_whitespace(value):
        """Remove whitespace from value.

        :param value: value string
        :type value: str
        :return: value with whitespace removed and lowercased
        :rtype: str
        """
        if (value.lower() == 'na' or
                value.lower() == 'n\a'or
                value.lower() == 'n/a'):
            value = ''
        value_str = re.sub(r'\d', '', value).replace(" ", "").replace("'", "")
        return re.sub(r'\s+', '', value_str).lower()


class GenerateVariable(object):
    """Class that generates variable index and prefix. """

    @staticmethod
    def create_index_variable(index, column):
        """Create a suffix for a variable name.

        :param index: suffix integer for the variable name
        :type index: int
        :param column: name of variable
        :type column: str
        :return: string of the form column_index
        :rtype: str
        """
        column = re.sub(r'\s+', '', column)
        return '{0}_{1}'.format(column, index)
