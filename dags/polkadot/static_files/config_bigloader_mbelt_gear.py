from google.cloud import bigquery

# write_disposition - this is for writing data to bigquery
# update_type - this is for taking data by increment from postgres
# 'update_type': 'increment' - if table in bigquery exists only
CHAIN = 'gear'
# WRITE_DISPOSITION = 'write_truncate'
WRITE_DISPOSITION = 'append'
# UPDATE_TYPE = 'full'
UPDATE_TYPE = 'increment'

TABLE_CONF = {
    'accounts': {
        'schema': [
            bigquery.SchemaField("network_id", "INT64"),
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("blake2_hash", "STRING"),
            bigquery.SchemaField("created_at_block_id", "INT64"),
            bigquery.SchemaField("killed_at_block_id", "INT64"),
            bigquery.SchemaField("judgement_status", "STRING"),
            bigquery.SchemaField("registrar_index", "INT64"),
            bigquery.SchemaField("row_id", "INT64"),
            bigquery.SchemaField("row_time", "TIMESTAMP"),
        ],
        # 'query_select': 'SELECT * FROM networks',
        'write_disposition': WRITE_DISPOSITION,
        'id_col_name': 'row_id',
        'update_type': UPDATE_TYPE,
        'file_size_limit_rows': 5_000_000,
    },

    'gear_smartcontracts': {
        'schema': [
            bigquery.SchemaField("network_id", "INT64"),
            bigquery.SchemaField("block_id", "INT64"),
            bigquery.SchemaField("extrinsic_id", "STRING"),
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("program_id", "STRING"),
            bigquery.SchemaField("expiration", "STRING"),
            bigquery.SchemaField("gas_limit", "STRING"),
            bigquery.SchemaField("init_payload", "STRING"),
            bigquery.SchemaField("init_payload_decoded", "STRING"),
            bigquery.SchemaField("code", "STRING"),
            bigquery.SchemaField("row_id", "INT64"),
            bigquery.SchemaField("row_time", "TIMESTAMP"),
        ],
        # 'query_select': 'SELECT * FROM extrinsics',
        'write_disposition': WRITE_DISPOSITION,
        'id_col_name': 'row_id',
        'update_type': UPDATE_TYPE,
        'file_size_limit_rows': 5_000_000,
    },

    'gear_smartcontracts_messages': {
        'schema': [
            bigquery.SchemaField("network_id", "INT64"),
            bigquery.SchemaField("block_id", "INT64"),
            bigquery.SchemaField("extrinsic_id", "STRING"),
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("program_id", "STRING"),
            bigquery.SchemaField("gas_limit", "STRING"),
            bigquery.SchemaField("payload", "STRING"),
            bigquery.SchemaField("payload_decoded", "STRING"),
            bigquery.SchemaField("value", "STRING"),
            bigquery.SchemaField("row_id", "INT64"),
            bigquery.SchemaField("row_time", "TIMESTAMP"),
        ],
        # 'query_select': 'SELECT * FROM extrinsics',
        'write_disposition': WRITE_DISPOSITION,
        'id_col_name': 'row_id',
        'update_type': UPDATE_TYPE,
        'file_size_limit_rows': 5_000_000,
    },

    'events': {
        'schema': [
            bigquery.SchemaField("network_id", "INT64"),
            bigquery.SchemaField("event_id", "STRING"),
            bigquery.SchemaField("block_id", "INT64"),
            bigquery.SchemaField("section", "STRING"),
            bigquery.SchemaField("method", "STRING"),
            bigquery.SchemaField("event", "STRING"),
            bigquery.SchemaField("row_id", "INT64"),
            bigquery.SchemaField("row_time", "TIMESTAMP"),
        ],
        # 'query_select': 'SELECT * FROM events',
        'write_disposition': WRITE_DISPOSITION,
        'id_col_name': 'row_id',
        'update_type': UPDATE_TYPE,
        'file_size_limit_rows': 5_000_000,
    },

    'extrinsics': {
        'schema': [
            bigquery.SchemaField("network_id", "INT64"),
            bigquery.SchemaField("extrinsic_id", "STRING"),
            bigquery.SchemaField("block_id", "INT64"),
            bigquery.SchemaField("success", "BOOLEAN"),
            bigquery.SchemaField("parent_id", "STRING"),
            bigquery.SchemaField("section", "STRING"),
            bigquery.SchemaField("method", "STRING"),
            bigquery.SchemaField("mortal_period", "INT64"),
            bigquery.SchemaField("mortal_phase", "INT64"),
            bigquery.SchemaField("is_signed", "BOOLEAN"),
            bigquery.SchemaField("signer", "STRING"),
            bigquery.SchemaField("tip", "INT64"),
            bigquery.SchemaField("nonce", "FLOAT64"),
            bigquery.SchemaField("ref_event_ids", "STRING"),
            bigquery.SchemaField("version", "INT64"),
            bigquery.SchemaField("extrinsic", "STRING"),
            bigquery.SchemaField("row_id", "INT64"),
            bigquery.SchemaField("row_time", "TIMESTAMP"),
        ],
        # 'query_select': 'SELECT * FROM events',
        'write_disposition': WRITE_DISPOSITION,
        'id_col_name': 'row_id',
        'update_type': UPDATE_TYPE,
        'file_size_limit_rows': 5_000_000,
    },
}