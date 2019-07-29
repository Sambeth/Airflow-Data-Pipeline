from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    sql_format = """
                    INSERT INTO {}
                    {};
                """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 data_source="",
                 *args, **kwargs):
        """
        :param redshift_conn_id:
        :param table: table name
        :param data_source: data to be inserted
        :param args:
        :param kwargs:
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.data_source = data_source

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f"Truncating {self.table}")

        sql_query = LoadFactOperator.sql_format.format(
            self.table,
            self.data_source
        )
        self.log.info(f"Running {sql_query}")
        redshift.run(sql_query)
