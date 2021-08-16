from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 query='',
                 expected_result='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.query=query
        self.expected_result=expected_result

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        connection = redshift.get_conn()
        cursor = connection.cursor()

        self.log.info('Starting data quality check ...')
        cursor.execute(self.query)
        result = cursor.fetchone()

        if result[0] != self.expected_result:
            raise ValueError(f'Data quality check failed, expected result was {self.expected_result}, actual result is {result[0]}!')

        else :
            self.log.info(f'Data quality check succeeded with {result} records as result!')

        cursor.close()