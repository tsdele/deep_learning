"""Pyspark storage tools functions"""
from datetime import datetime as dt
import logging
import os
import tempfile
import time
import uuid
import simplejson as json

from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from google.cloud import bigquery, storage

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)-7s - %(message)s',
)

CONSOLE_HANDLER = logging.StreamHandler()
CONSOLE_HANDLER.setLevel(logging.DEBUG)

LOGGER = logging.getLogger('cluster_pairs')
LOGGER.addHandler(CONSOLE_HANDLER)

GS_OBJ = storage.Client()


def bucket_exists(bucket_name):
    """Check if a bucket exists.
    :param bucket_name: The bucket name to check for existance.
    :type bucket_name: str
    :return: True if bucket exists in project.
    :rtype: bool
    """
    if GS_OBJ.lookup_bucket(bucket_name):
        return True
    return False


def copy_json_to_bq(json_list,
                    project_id,
                    dataset_id,
                    table_id,
                    bucket=None,
                    prefix=None):
    """Upload a list of json dictionaries to target BigQuery table.
    Data is saved to Google storage location first,
    as to avoid the streaming API.
    Usage:
        example_json_list = [
            {'field': 1, 'nested': [
                {'inside1': "nested field value"},
                {'inside2': "second nested val"}
            ]},
            {'field': 2, 'nested': [
                {'inside1': "for second entry. List can be any length"}
            ]},
        ]
        copy_json_to_bq(
            example_json_list,
            'dotmodus',
            'tmp',
            'test_table'
        )
    :param json_list: List of json objects following BigQuery
      newline delimited json convention
    :type json_list: list
    :param project_id: Project id for google BigQuery
    :type project_id: str
    :param dataset_id: BigQuery target dataset_id
    :type dataset_id: str
    :param table_id: BigQuery target table name
    :type table_id: str
    :param bucket: Google Storage bucket where file should be dropped off.
      If None, then defaults to '<project_id>_tmp'.
      Creates bucket if it does not exist already.
    :type bucket: str
    :param prefix: Google Storage file prefix where file should be dropped off
    If None, then defaults to timestamp prefix.
    :type prefix: str
    :return: None
    """
    # autoassign bucket if left as None
    if bucket is None:
        bucket = "user_dedupe_tmp"

    # create bucket if it does not exist already
    if not bucket_exists(bucket):
        GS_OBJ.create_bucket(bucket)
        LOGGER.info("created bucket {} in project {}".format(
            bucket,
            project_id
        ))

    if prefix is None:
        prefix = "{}_".format(str(dt.now()).replace(' ', '_'))
        LOGGER.info("prefix set as current datetime:  {}".format(prefix))

    LOGGER.info("Uploading into storage.  bucket = {}; prefix = {}".format(
        bucket,
        prefix
    ))

    file_location_dict = store_data_to_bucket(json_list, bucket, prefix)
    print(file_location_dict)

    filename = 'gs://{}/{}'.format(file_location_dict['bucket'],
                                   file_location_dict['name'])

    LOGGER.info("Filename:  {}".format(filename))
    LOGGER.info("Loading into BQ: {}.{}".format(dataset_id, table_id))

    load_table_from_gcs(
            project_id, dataset_id, table_id, filename,
            write_disposition='WRITE_TRUNCATE',
            source_format='NEWLINE_DELIMITED_JSON'
    )
    LOGGER.info("Data loaded to BigQuery.")


def store_data_to_bucket(items, bucket, prefix=None):
    """Store key/value items received as JSON and uploads to bucket.
    Temporarily creates a file to upload
    :param items: list of dictionary items
    :type items: list
    :param bucket: name
    :type bucket: str
    :param prefix: optional prefix to add to file location
    :type prefix: str
    :return: all metadata related to the stored file.
      filename = file_storage['name']
    :rtype: dict
    """
    credentials = GoogleCredentials.get_application_default()
    api = build('storage', 'v1', credentials=credentials)
    LOGGER.info(
        'Storage:storeDataToBucket storing {} rows to bucket {}'.format(
            len(items),
            bucket
        )
    )

    # make some temporary files.
    temp_dir = tempfile.mkdtemp(prefix='gcs_')
    file_name = '{}.json'.format(int(time.time()))
    file_local = os.path.join(temp_dir, file_name)
    LOGGER.debug('Storage:storeDataToBucket {} -> {}'.format(
        file_local, file_name)
    )

    filedata = [
        json.dumps(item, use_decimal=True, default=dt.isoformat)
        for item in items
    ]

    with open(file_local, 'w') as file_handle:
        file_handle.write('\n'.join(filedata))

    # upload
    params = {
        'bucket': bucket,
        'name': '{}{}'.format(prefix or '', file_name),
        'media_body': file_local,
    }
    LOGGER.debug('Storage:storeDataToBucket params {}'.format(params))

    for _ in range(1, 4):
        try:
            file_storage = api.objects().insert(**params).execute()
        except HttpError as err:
            LOGGER.exception('storing data error on try {}'.format(_))
            LOGGER.exception('error: {}'.format(err))
            if _ == 3:
                raise  # TODO raise something?
            time.sleep(30)
        else:
            os.remove(file_local)
            LOGGER.debug(
                'Storage:storeDataToBucket local file removed {}'.format(
                    file_local
                )
            )
            break

    LOGGER.debug('Storage:saveRowsToBucket {}'.format(file_storage))
    LOGGER.info('Storage:saveRowsToBucket done')
    return file_storage


def wait_for_complete(job):
    """Wait for a job to complete.
    :param job: A job object created by an api call.
    :type job: bigquery.job
    :return: Just pass the job through
    :rtype: bigquery.job
    """
    if job.errors:
        LOGGER.error(job.errors)

    while job.state != 'DONE':
        time.sleep(5)
        LOGGER.info("JOB '{}': {}".format(job.name, job.state))
        job.reload()

    LOGGER.info(job.state)


def load_table_from_gcs(project_id,
                        dataset_id,
                        table_id,
                        source_uri_pattern,
                        create_disposition="CREATE_IF_NEEDED",
                        write_disposition="WRITE_TRUNCATE",
                        source_format='NEWLINE_DELIMITED_JSON',
                        skip_leading_rows=0):
    """Load data from google storage into BigQuery.
    :param project_id: project_id name
    :type project_id: str
    :param dataset_id: dataset_id name
    :type dataset_id: str
    :param table_id: table_id name
    :type table_id: str
    :param source_uri_pattern: UIR of form 'gs://bucket/file/abc-*
    :type source_uri_pattern: str
    :param create_disposition: default CREATE_IF_NEEDED
    :type create_disposition: str
    :param write_disposition: default WRITE_TRUNCATE (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
    :type write_disposition: str
    :param source_format: default NEWLINE_DELIMITED_JSON, else is CSV
    :type source_format: str
    :param skip_leading_rows: number of leading rows to skip for CSV format
    :type skip_leading_rows: int
    :return: Job ID
    :rtype: str
    """
    client = bigquery.client.Client(project=project_id)
    destination_table = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.create_disposition = create_disposition
    if skip_leading_rows:
        job_config.skip_leading_rows = skip_leading_rows
    job_config.source_format = source_format
    job_config.write_disposition = write_disposition

    load_job = client.load_table_from_uri(
        source_uri_pattern, destination_table, job_config=job_config,
        job_id_prefix=str(uuid.uuid4()))

    load_job.result()

