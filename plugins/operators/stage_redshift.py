#from airflow.typing_extensions import TypeVarTuple
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields=("s3_key", "execution_date", )

    COPY_SQL = """
    COPY {table}
    FROM '{s3_path}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{access_secret}'
    json 'auto ignorecase'
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                create_statement="",
                s3_bucket="",
                s3_key="",
                execution_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.create_statement=create_statement
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.execution_date=execution_date

    def execute(self, context):
        '''
        Stage data from S3 to a given Redshift table.
        '''

        aws_hook=AwsHook(self.aws_credentials_id, client_type="redshift")
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run(self.create_statement)
        redshift.run(f'TRUNCATE TABLE {self.table}')

        #Only process the given run date if a run date is passed. 
        if self.execution_date:
            date = datetime.strptime(self.execution_date, '%Y-%m-%d')
            self.s3_key=f"{self.s3_key}/{date.year}/{date.month}/{self.execution_date}-events.json"


        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f'Loading data from {s3_path}')
        formatted_sql = StageToRedshiftOperator.COPY_SQL.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            access_secret=credentials.secret_key
        )

        self.log.info(f'Data loaded to {self.table} table')
        redshift.run(formatted_sql)