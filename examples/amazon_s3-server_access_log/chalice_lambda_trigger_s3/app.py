from chalice import Chalice
import pandas as pd
import re

app = Chalice(app_name='s3_lambda_trigger')


@app.on_s3_event('logs', prefix='s3/', events=['s3:ObjectCreated:*'])
def handler(event):
    patterns = [
        "(\S+)\s",
        "(\S+)\s",
        "\[([^\]]+)\]\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        '(-|"-"|"\S+ \S+ (?:-|\S+)")\s',
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        '(-|"[^"]+")\s',
        '(-|"[^"]+")\s',
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)\s",
        "(\S+)"
    ]
    column_names = [
        'bucket_owner',
        'bucket',
        'time',
        'remote_ip',
        'requester',
        'request_id',
        'operation',
        'key',
        'request_uri',
        'http_status',
        'error_code',
        'bytes_sent',
        'object_size',
        'total_time',
        'turn_around_time',
        'referer',
        'user_agent',
        'version_id',
        'host_id',
        'signature_version',
        'cipher_suite',
        'authentication_type',
        'host_header',
        'tls_version'
    ]

    print(event.bucket, event.key)

    df = pd.read_csv(f's3://{event.bucket}/{event.key}', sep='\t', header=None)
    df.rename(columns={0: "data"}, inplace=True)

    log_pattern = re.compile(re.compile("".join(patterns)))
    df = df.data.str.extract(log_pattern)
    df.columns = column_names

    df.to_parquet(f's3://{event.bucket}/convert/{event.key}.parquet')

    print(f's3://{event.bucket}/convert/{event.key}.parquet')
