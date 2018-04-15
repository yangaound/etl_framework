import time

from dbman import Manipulator
from .tasks import TaskMixin


class Loading(object):
    """ Abstract class that load data from a given table into a file-like destination or database."""

    def setup(self, task, job_config):
        """prepare environment/runtime related dependencies"""

    def teardown(self, task, job_config):
        """recovery environment or free runtime related resource"""

    def load(self, task, job_config, table):
        """loading the table"""
        raise NotImplementedError('method `load()` must be implemented')

    def get_report(self, task, job_config):
        return self.__dict__

    @staticmethod
    def _check_params(task, job_config):
        assert isinstance(task, TaskMixin), '`task` must be an instance of derived class of `etl.tasks.TaskMixin`'
        assert isinstance(job_config, dict), '`job_config` must be a dict instance'


class NoneLoading(Loading):
    def load(self, *args, **kwargs):
        pass


class DBLoading(Loading):
    def __init__(self):
        self.manipulator = None   # DB manipulator object
        self.affected_row = 0

    def setup(self, task, job_config):
        self.manipulator = Manipulator(db_config=job_config['db_config'], db_label=task.config['loading']['db_config_label'])

    def teardown(self, task, job_config):
        if self.manipulator:
            self.manipulator.close()

    def get_report(self, task, job_config):
        return {
            "affected": self.affected_row,
        }

    def load(self, task, job_config, table):
        loading_config = task.config['loading']
        tick = time.time()
        table = self._cut_invalid_fields(task, loading_config, table)
        self._load(task, loading_config, table)

        msg = "{cls_name}<time_cost={time_cost}, table_name={table_name}, affected_row={affected_row}, import_mode={import_mode}, pk={pk}>".format(
            cls_name=self.__class__.__name__,
            time_cost="%.3f" % (time.time() - tick),
            table_name=loading_config['table_name'],
            affected_row=self.affected_row,
            import_mode=loading_config['import_mode'],
            pk=loading_config.get('pk') or (),
        )
        task.logger.info(msg)

    def _cut_invalid_fields(self, task, loading_config, table):
        header1 = table.header()
        header2 = self._get_table_header(loading_config)
        diff = set(header1) - set(header2)
        if diff:
            table = table.cutout(*diff)
            msg = """
            source header: %s
            and destination header: %s
            are different in task '%s'
            diff: %s""" % (header1, header2, task.name, diff)
            task.logger.warning(msg)
        return table

    def _load(self, task, loading_config, table):
        self.affected_row = self.manipulator.todb(table,
                                                  table_name=loading_config['table_name'],
                                                  mode=loading_config['import_mode'],
                                                  duplicate_key=loading_config.get('pk') or (),
                                                  )

    def _get_table_header(self, loading_config):
        sql = "SELECT * FROM {table_name} LIMIT 1;".format(**loading_config)
        table = self.manipulator.fromdb(sql)
        return table.header()


class CountedDBLoading(DBLoading):
    def __init__(self):
        super(CountedDBLoading, self).__init__()
        self.affected_row = 0
        self.inserted_row = 0
        self.replaced_row = 0

    def get_report(self, task, job_config):
        return {
            "affected": self.affected_row,
            "inserted": self.inserted_row,
            "replaced": self.replaced_row,
        }

    def load(self, task, job_config, table):
        loading_config = task.config['loading']
        tick = time.time()
        table = self._cut_invalid_fields(task, loading_config, table)

        before_row_count = self._get_row_count(loading_config)
        self._load(task, loading_config, table)
        after_row_count = self._get_row_count(loading_config)

        self.inserted_row = after_row_count - before_row_count
        self.replaced_row = table.nrows() - self.inserted_row

        msg = "{cls_name}<time_cost={time_cost}, table_name={table_name}, affected_row={affected_row}, import_mode={import_mode}, pk={pk}>".format(
            cls_name=self.__class__.__name__,
            time_cost="%.3f" % (time.time() - tick),
            table_name=loading_config['table_name'],
            affected_row=self.affected_row,
            import_mode=loading_config['import_mode'],
            pk=loading_config.get('pk') or (),
        )
        task.logger.info(msg)

    def _get_row_count(self, loading_config):
        sql = "SELECT COUNT(*) FROM {table_name};".format(**loading_config)
        cursor = self.manipulator.cursor()
        cursor.execute(sql)
        sequence = cursor.fetchall()
        cursor.close()
        return sequence[0][0]