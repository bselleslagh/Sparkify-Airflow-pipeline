from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                destination_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table

    def execute(self, context):
        '''
        This operator will load fact data into a given target table.
        The SQL is contained in the hepler class sql_queries
        '''
        
        self.log.info(f'Starting to append data to fact table {self.destination_table}')
        redshift_hook=PostgresHook(self.redshift_conn_id)

        check_table_exists = redshift_hook.run(f"select count(*) from PG_TABLE_DEF where tablename = '{self.destination_table}'")
        
        if check_table_exists == 0:
            redshift_hook.run(f'CREATE TABLE {self.destination_table} AS ' + SqlQueries.songplay_table_insert)

        else:
            redshift_hook.run(f'INSERT INTO {self.destination_table} (' + SqlQueries.songplay_table_insert + ' )')
