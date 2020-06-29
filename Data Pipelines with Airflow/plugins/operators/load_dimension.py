from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 truncate_first=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_first = truncate_first

    def execute(self, context):
        
        self.redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_first:
            self.log.info(f"Truncating data in {self.table} dimension...")
            self.redshift.run(f"TRUNCATE {self.table};")
        self.log.info("Inserting data from staging into {} dimension...".format(self.table))
        self.redshift.run(f"INSERT INTO {self.table} {self.sql};")
        self.log.info("Inserting data from staging into {} dimension complete".format(self.table))
        
