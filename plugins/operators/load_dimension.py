from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tableName="",
                 truncateTable=False,
                 SQLquery="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.tableName=tableName
        self.redshift_conn_id = redshift_conn_id
        self.truncateTable=truncateTable
        self.SQLquery=SQLquery
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncateTable:
            self.log.info(f"Flag set to TRUE. Running delete statement on table {self.tableName}")
            redshift.run("DELETE FROM {}".format(self.tableName))
            
        self.log.info(f"Running query to load data into Dimension Table {self.tableName}")
        redshift.run(self.SQLquery)
        self.log.info(f"Dimension Table {self.tableName} loaded succeeded") 
        
     
