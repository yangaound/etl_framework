#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing
import os
import threading
import traceback
import logging
import datetime
import time
import fcntl

from dateutil.relativedelta import relativedelta

from .util import datetime2str, str2datetime
from .util import ETLException, Error


DIR = os.path.dirname(os.path.abspath(__file__))
HIS_UPDATE_TIME_DIR = os.path.join(DIR, '.updatetime')
LOCK_FILE_DIR = os.path.join(DIR, '.lockfile')


if not os.path.exists(HIS_UPDATE_TIME_DIR):
    os.makedirs(HIS_UPDATE_TIME_DIR)
    print 'Make Directory: ', HIS_UPDATE_TIME_DIR
if not os.path.exists(LOCK_FILE_DIR):
    os.makedirs(LOCK_FILE_DIR)
    print 'Make Directory: ', LOCK_FILE_DIR


class TaskMixin(object):
    def __init__(self, dispatcher, config,
                 extractor, transformer, loader, monitor_handler):
        self.dispatcher = dispatcher
        self.job_config = dispatcher.job_config
        self.args = dispatcher.args
        self.config = config
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.monitor_handler = monitor_handler
        self.lock_file_obj = None
        self.init_timestamp = time.time()
        self.logger = self.__make_logger()

    def delegate_extractor(self):
        return self.extractor.extract(self, self.job_config)

    def delegate_transformer(self, table):
        return self.transformer.transform(self, self.job_config, table)

    def delegate_loader(self, table):
        return self.loader.load(self, self.job_config, table)

    def delegate_monitor_handler(self):
        if self.monitor_handler:
            extractor_report = self.extractor.get_report(self, self.job_config)
            transformer_report = self.transformer.get_report(self, self.job_config)
            loader_report = self.loader.get_report(self, self.job_config)
            return self.monitor_handler.handle(self, self.job_config, extractor_report, transformer_report, loader_report)

    def logs(self):
        threading.current_thread().name = self.config['id']
        msg = 'Started Task extractor:%s, transformer:%s, loader:%s, monitor:%s' % (
            self.config['extraction']['extraction_class'],
            self.config['transformation']['transformation_class'],
            self.config['loading']['loading_class'],
            self.job_config.get('monitor') and self.job_config['monitor']['handler_class']
        )
        self.logger.info(msg)

    def run_task(self):
        self.logs()
        try:
            self.extractor.setup(self, self.job_config)
            table = self.delegate_extractor()
            assert table is not None, 'extractor must be return a table'
            table = self.delegate_transformer(table)
            assert table is not None, 'transformer must be return a table'
            self.__open_and_lockf()
            self.loader.setup(self, self.job_config)
            self.delegate_loader(table)
        except ETLException as (Error.no, Error.msg):
            self.logger.error('err_no: %d, err_msg:%s' % (Error.no, Error.msg))
        except Exception as e:
            Error.no, Error.msg = Error.RumtimeErrno, str(e)
            self.logger.error(traceback.format_exc())
            raise
        finally:
            self.__close_and_unlockf()
            self.extractor.teardown(self, self.job_config)
            self.loader.teardown(self, self.job_config)
            self.delegate_monitor_handler()

    def __make_logger(self):
        name = self.config['id']
        level = self.job_config['log_level']
        filename = os.path.join(self.job_config['log_dir'], self.config['id'])
        format = "[%(levelname)s][%(asctime)s][%(process)s] %(message)s"
        # make a logger
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()) if isinstance(level, basestring) else level)
        formatter = logging.Formatter(format)
        handler = logging.FileHandler(filename)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def __open_and_lockf(self):
        # do noting if async
        if not self.config['loading'].get('sync', True):
            return
        # sync by default
        db_config_label, db_table_name = self.config['loading']['db_config_label'], self.config['loading']['table_name']
        lock_file_dir = os.path.join(LOCK_FILE_DIR, db_config_label)
        lock_file_path = os.path.join(lock_file_dir, db_table_name)
        if not os.path.exists(lock_file_dir):
            os.makedirs(lock_file_dir)
            print 'Make Directory: ', lock_file_dir

        self.lock_file_obj = open(lock_file_path, 'w')
        try:
            fcntl.lockf(self.lock_file_obj.fileno(), fcntl.LOCK_EX)
        except Exception as e:
            raise ETLException(Error.DropTaskErrno, "task '%s': %s" % (self.name, str(e)))

    def __close_and_unlockf(self):
        # do noting if async
        if not self.config['loading'].get('sync', True):
            return
        # sync by default
        try:
            fcntl.lockf(self.lock_file_obj.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        finally:
            try:
                self.lock_file_obj and self.lock_file_obj.close()
            except Exception:
                pass


class ProcessTask(multiprocessing.Process, TaskMixin):
    def __init__(self, dispatcher, config, *args, **kwargs):
        multiprocessing.Process.__init__(self, name=config['id'])
        TaskMixin.__init__(self, dispatcher, config, *args, **kwargs)

    def run(self):
        self.run_task()


class ThreadTask(threading.Thread, TaskMixin):
    def __init__(self, dispatcher, config, *args, **kwargs):
        threading.Thread.__init__(self, name=config['id'])
        TaskMixin.__init__(self, dispatcher, config, *args, **kwargs)

    def run(self):
        self.run_task()


class TimedDBTask(ProcessTask):
    def __init__(self, dispatcher, config, *args, **kwargs):
        super(TimedDBTask, self).__init__(dispatcher, config, *args, **kwargs)
        self.his_update_time_file_path = os.path.join(HIS_UPDATE_TIME_DIR, self.name)

    def get_his_update_time(self):
        data_time_start = data_time_end = datetime.datetime.now()
        # time from configuration file
        if 'time_column' in self.config['extraction'] and self.config['extraction']['time_column'].get('time_delta'):
            number, units = self.config['extraction']['time_column']['time_delta'].strip().split(' ', 1)
            data_time_start = data_time_end + relativedelta(**{units: int(number)})

        # time read from history file
        try:
            with open(self.his_update_time_file_path) as fp:
                time_str = fp.read().strip()
            the_last_update_time = str2datetime(time_str)
        except Exception as e:
            self.logger.warning(str(e))
            the_last_update_time = data_time_end - datetime.timedelta(minutes=10)

        return the_last_update_time if the_last_update_time < data_time_start else data_time_start

    def store_his_update_time(self, data_time_end):
        try:
            with open(self.his_update_time_file_path, 'w') as fp:
                fp.write(datetime2str(data_time_end))
        except Exception as e:
            raise ETLException(Error.RumtimeErrno, str(e))