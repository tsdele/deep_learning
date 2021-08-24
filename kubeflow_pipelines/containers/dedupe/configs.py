"""Config file for the user deduplication jobs"""

DEDUPE_CONFIGS_COLUMNS = ['full_name', 'first_name', 'last_name',
                          'email_address', 'home_telephone_number',
                          'work_telephone_number', 'mobile_telephone_number',
                           'sourced_from', 'organization_id',
                          'record_fingerprint']

PROJECT_ID = '{project}'
DATASET_ID = '{dataset}'
TABLE_ID = '{source_table}'
TARGET_TABLE = '{target_table}'
MANUAL_PROFILE_DATASET_ID = '{manual_profile_dataset_id}'
MANUAL_PROFILE_TABLE_ID = '{manual_profile_table_id}'

