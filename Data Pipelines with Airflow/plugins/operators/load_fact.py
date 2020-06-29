from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 truncate_first=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_first = truncate_first

    def execute(self, context):
        
        self.redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_first:
            self.log.info(f"Truncating data in {self.table} fact...")
            self.redshift.run(f"TRUNCATE {self.table};")
        self.log.info(f"Inserting data from staging into {self.table} fact...")
        self.redshift.run(f"INSERT INTO {self.table} {self.sql};")
        self.log.info(f"Inserting data from staging into {self.table} fact complete")
        
