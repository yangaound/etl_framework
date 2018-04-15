
import time

from dbman import Manipulator
from .tasks import TaskMixin
from .util import DT_PATTERN, datetime2str


class Extraction(object):
    """ Abstract class that extract data(a table) from a file-like source or database"""

    def setup(self, task, job_config):
        """prepare environment/runtime related dependencies"""

    def teardown(self, task, job_config):
        """recovery environment or free runtime related resource"""

    def extract(self, task, job_config):
        """
        :return: a table from a data_source it can be a file-like source or database
        """
        raise NotImplementedError('method `extract()` must be implemented')

    def get_report(self, task, job_config):
        return {}


class TimedDBExtraction(Extraction):
    def __init__(self):
        self.manipulator = None  # DB manipulator object
        self.selected_row = 0    # Selected_row from source table
        self.data_time_start = None
        self.data_time_end = None

    def setup(self, task, job_config):
        self.manipulator = Manipulator(db_config=job_config['db_config'], db_label=task.config['extraction']['db_config_label'])

    def teardown(self, task, job_config):
        if self.manipulator:
            self.manipulator.close()

    def get_report(self, task, job_config):
        return {
            "selected": self.selected_row,
            "data_time_start": self.data_time_start,
            "data_time_end": self.data_time_end,
        }

    def extract(self, task, job_config):
        tick = time.time()
        self._check_params(task, job_config)
        self.data_time_start = self.data_time_end = task.get_his_update_time()

        extraction_config = task.config['extraction']
        if extraction_config.get('query_sql'):
            sql = extraction_config['query_sql']
        else:
            sql = self._make_query_sql(task, extraction_config)

        table = self.manipulator.fromdb(sql, latency=False)
        self.selected_row = table.nrows()

        the_first_row, the_last_row = None, None
        if (self.selected_row > 0) and ('time_column' in extraction_config):
            time_column_name = extraction_config['time_column']['name']
            the_first_row = table.dicts()[0]
            _tmp = table.skip(self.selected_row)
            _tmp = _tmp.pushheader(table.header())
            the_last_row = _tmp.dicts()[0]
            self.data_time_start = the_first_row[time_column_name]
            self.data_time_end = the_last_row[time_column_name]

        msg = """
        {cls}<
        selected_row={selected_row},
        data_time_start={data_time_start},
        data_time_end={data_time_end},
        time_cost={time_cost},
        sql={sql},
        the_first_row_data={the_first_row},
        the_last_row_data={the_last_row}>
        """.format(
            cls=self.__class__.__name__,
            time_cost="%.3f" % (time.time() - tick),
            selected_row=self.selected_row,
            data_time_start=self.data_time_start,
            data_time_end=self.data_time_end,
            sql=sql,
            the_first_row=the_first_row,
            the_last_row=the_last_row,
        )
        task.logger.info(msg)
        return table

    def _make_query_sql(self, task, extraction_config):
        if 'time_column' in extraction_config and extraction_config['time_column'].get('repl'):
            data_time_start = DT_PATTERN.sub(extraction_config['time_column']['repl'], datetime2str(self.data_time_start))
        else:
            data_time_start = datetime2str(self.data_time_start)
        return """SELECT * FROM {table_name} WHERE {time_column_name} >='{data_time_start}' ORDER BY {time_column_name}""".format(
            table_name=extraction_config['table_name'],
            time_column_name=extraction_config['time_column']['name'],
            data_time_start=data_time_start,
        )