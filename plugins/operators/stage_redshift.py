from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql_format = """
        COPY {}
        FROM '{}'
        with credentials
        'aws_access_key_id={};aws_secret_access_key={}'
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 copy_options=[],
                 *args, **kwargs):
        """
        :param redshift_conn_id:
        :param aws_credentials_id:
        :param table: table name
        :param s3_bucket:
        :param s3_key:
        :param copy_options:
        :param args:
        :param kwargs:
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_options = copy_options

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.copy_options:
            copy_options = '\n\t\t\t'.join(self.copy_options)
        else:
            copy_options = ''

        self.log.info("Truncating redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from s3 bucket to redshift")
        key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, key)
        sql_query = StageToRedshiftOperator.sql_format.format(
            self.table,
            s3_path,
            credentials.access_keys,
            credentials.secret_key,
            copy_options
        )
        self.log.info(f"Running {sql_query}")
        redshift.run(sql_query)
