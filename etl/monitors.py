import time
import traceback

import petl

from .util import Error
from dbman import Manipulator


class Handler(object):
        """ Abstract class that handle given data"""

        def setup(self, task, job_config):
            """prepare environment/runtime related dependencies"""

        def teardown(self, task, job_config):
            """recovery environment or free runtime related resource"""

        def handle(self, task, job_config, extractor_report, transformer_report, loader_report):
            """
            :return: a table from a data_source it can be a file-like source or database
            """
            raise NotImplementedError('method `handle()` must be implemented')


class NoneHandler(Handler):
    def handle(self, task, job_config, extractor_report, transformer_report, loader_report):
        pass


class DBRPMonitorHandler(Handler):
    def __init__(self):
        self.manipulator = None

    def __init__(self):
        self.manipulator = None   # DB manipulator object
        self.affected_row = 0

    def setup(self, task, job_config):
        self.manipulator = Manipulator(db_config=job_config['db_config'], db_label=job_config['monitor']['db_config_label'])

    def teardown(self, task, job_config):
        if self.manipulator:
            self.manipulator.close()

    def handle(self, task, job_config, extractor_report, transformer_report, loader_report):
        try:
            self.setup(task, job_config)
            self._handler(task, job_config, extractor_report, transformer_report, loader_report)
            self.teardown(task, job_config)
        except Exception:
            task.logger.error(traceback.format_exc())
            raise

    def _handler(self, task, job_config, extractor_report, transformer_report, loader_report):
        if extractor_report['data_time_end']:
            task.store_his_update_time(extractor_report['data_time_end'])

        monitor = {
            'error_code': Error.no,
            # 'error_msg': Error.msg,
            'frequency': task.config.get('group') or task.config['id'],
            'table_name': task.name,
            'cost': "%.2f" % (time.time() - task.init_timestamp),
            'start_time': extractor_report['data_time_start'],
            'end_time': extractor_report['data_time_end'],
            'selected': extractor_report['selected'],
            'inserted': loader_report['inserted'],
            'replaced': loader_report['replaced'],
        }
        table = petl.fromdicts([monitor, ])
        self.manipulator.todb(table, table_name=job_config['monitor']['table_name'], mode=job_config['monitor']['import_mode'])

        msg = "{cls}<selected={selected}, inserted={inserted}, replaced={replaced}, affected={affected}, error_code={error_code}>".format(
            cls=self.__class__.__name__,
            error_code=Error.no,
            selected=extractor_report['selected'],
            inserted=loader_report['inserted'],
            affected=loader_report['affected'],
            replaced=loader_report['replaced'],
        )
        task.logger.info(msg)