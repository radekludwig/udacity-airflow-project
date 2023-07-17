from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import operator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def get_truth(self, inp, relate, cut):
        ops = {
            '>': operator.gt,
            '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le,
            '==': operator.eq,
            '!=': operator.ne
        }
        return ops[relate](inp, cut)
    def execute(self, context):
        self.log.info(f'Running checks on tables')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        for i, check in enumerate(self.checks):
            for check_sql, operator, expected_result in check.items():
                check_result = redshift.get_records(check_sql)
                if self.get_truth(check_result, operator, expected_result) is False:
                    raise ValueError(f'Data quality check nr {i} failed')
        self.log.info(f'Checks passed')
