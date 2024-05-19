from airflow.providers.amazon.aws.hooks.s3 import S3Hook
 def retrieve_json_from_s3(bucket_name, key, output_key, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    content = s3_hook.read_key(key, bucket_name=bucket_name)
    # Push the content to XCom for the next task to retrieve
    kwargs['ti'].xcom_push(key=output_key, value=content)