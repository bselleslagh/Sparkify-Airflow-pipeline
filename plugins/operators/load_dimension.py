from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                destination_table="",
                truncate_table="",
                query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.truncate_table=truncate_table
        self.query=query

    def execute(self, context):
        '''
        This operator will load fact tables.
        Given the query and destination table as input.
        The truncate table parameter must be set to True,
        if the table needs to be cleaned before load.
        '''
        self.log.info(f'Starting to process fact table {self.destination_table}')
        redshift_hook=PostgresHook(self.redshift_conn_id)
        
        if self.truncate_table:
            redshift_hook.run(f'TRUNCATE TABLE {self.destination_table}')

        insert_statement = f'INSERT INTO {self.destination_table} '
        redshift_hook.run(insert_statement + self.query)

        self.log.info('Loading fact table complete')
