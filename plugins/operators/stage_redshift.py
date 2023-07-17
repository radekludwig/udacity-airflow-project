from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql_copy = '''
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}'
    '''
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.json_path = json_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        if self.json_path == '':
            formatted_copy_query = StageToRedshiftOperator.sql_copy.format(self.table, s3_path, credentials.access_key,\
                                                                  credentials.secret_key, 'auto')
        else:
            formatted_copy_query = StageToRedshiftOperator.sql_copy.format(self.table, s3_path, credentials.access_key,\
                                                                  credentials.secret_key, self.json_path)
        self.log.info(f'Running copy query on {self.table} table')
        redshift.run(formatted_copy_query)

