from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    sql_format = """
            INSERT INTO {}
            {};
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 truncate=False,
                 data_source="",
                 *args, **kwargs):
        """
        :param redshift_conn_id:
        :param table: table name
        :param truncate:
        :param data_source: data to be inserted
        :param args:
        :param kwargs:
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
        self.data_source = data_source

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating {self.table} table")
            redshift.run(f"DELETE FROM {self.table}")

        sql_query = LoadDimensionOperator.sql_format.format(
            self.table,
            self.data_source
        )
        self.log.info(f"Running {self.sql_query}")
        redshift.run(sql_query)
