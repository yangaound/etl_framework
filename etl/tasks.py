#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing
import os
import threading
import traceback
import logging
import time


class TaskMixin(object):
    def __init__(self, dispatcher, config,
                 extractor, transformer, writer, monitor_handler):
        self.dispatcher = dispatcher
        self.job_config = dispatcher.job_config
        self.args = dispatcher.args
        self.config = config
        self.extractor = extractor
        self.transformer = transformer
        self.writer = writer
        self.monitor_handler = monitor_handler
        self.lock_file_obj = None
        self.init_timestamp = time.time()
        self.logger = self.__make_logger()

    def delegate_extractor(self):
        return self.extractor.extract(self, self.job_config)

    def delegate_transformer(self, table):
        return self.transformer.transform(self, self.job_config, table)

    def delegate_loader(self, table):
        return self.writer.load(self, self.job_config, table)

    def delegate_monitor_handler(self):
        if self.monitor_handler:
            extractor_report = self.extractor.get_report(self, self.job_config)
            transformer_report = self.transformer.get_report(self, self.job_config)
            loader_report = self.writer.get_report(self, self.job_config)
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
            self.lock_resource()
            self.writer.setup(self, self.job_config)
            self.delegate_loader(table)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            raise
        finally:
            self.unlock_resource()
            self.extractor.teardown(self, self.job_config)
            self.writer.teardown(self, self.job_config)
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

    def lock_resource(self):
        pass

    def unlock_resource(self):
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
