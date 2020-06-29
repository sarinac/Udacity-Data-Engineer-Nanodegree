from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 jsonpath="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.jsonpath = jsonpath
        if self.jsonpath != "auto":
            self.jsonpath = f"s3://{self.s3_bucket}/{self.jsonpath}"
        

    def execute(self, context):
 
        self.redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.aws_hook = AwsHook(self.aws_credentials_id)
        credentials = self.aws_hook.get_credentials()

        copy_query = """
            COPY {table}
            FROM 's3://{s3_bucket}/{s3_key}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            region '{region}'
            json '{jsonpath}'
            ignoreheader 1;
        """.format(
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            region=self.region,
            jsonpath=self.jsonpath,
        )

        self.redshift_hook.run(f"TRUNCATE {self.table};")
        self.log.info('Staging from S3 to Redshift...')
        self.redshift_hook.run(copy_query)
        self.log.info("Staging from S3 to Redshift complete...")





