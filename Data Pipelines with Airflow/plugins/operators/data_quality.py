from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Checking data quality...")
        
        for table in self.tables:
            
            # Check number of records in table
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table};")        
            if records is None or len(records[0]) < 1:
                self.log.error(f"No records present in table {table}")
                raise ValueError(f"No records present in table {table}")
        
        self.log.info("Checking data quality complete")
