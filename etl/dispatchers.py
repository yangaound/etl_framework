#!/usr/bin/env python
# -*- coding: utf-8 -*-

import signal
import sys
import traceback
import time
import os
import logging

import yaml

from .util import import_string

tasks = list()


class Dispatcher(object):
    err_logger_filename = 'etl'
    err_logger = None

    def __init__(self, args):
        self.args = args
        self.job_config = None
        self.tasks_config = list()

    def process(self):
        tick = time.time()

        with open(self.args.f, 'r') as fp:
            self.job_config = yaml.load(fp)

        if not os.path.exists(self.job_config['log_dir']):
            os.makedirs(self.job_config['log_dir'])
            print 'Make Directory: ', self.job_config['log_dir']

        # does basic configuration for logging
        logging.basicConfig(
            level=self.job_config['log_level'].upper(),
            filename=os.path.join(self.job_config['log_dir'], '%s.%s' % (self.err_logger_filename, time.strftime('%Y%m%d'))),
            format="[%(levelname)s][%(asctime)s][%(process)d][%(threadName)s] %(message)s",
            stream=sys.stdout,
        )
        self.err_logger = logging.getLogger()
        self.err_logger.addHandler(logging.StreamHandler(stream=sys.stdout))
        self.__class__.err_logger = self.err_logger

        self.err_logger.info("Accepted conmand: %s" % str(sys.argv))
        self.err_logger.info("Using config path: %s" % self.args.f)

        # remove tasks which group value is 'ignore'
        tasks_config = filter(lambda dic: (dic.get('group') != 'ignore'), self.job_config['tasks'])

        # lookup config by task id or group name
        self._lookup_config(tasks_config)

        # fetch config which group or id in command-line
        for dic in tasks_config:
            if ('all' == self.args.task) or \
                    (dic['group'] == self.args.task) or\
                    (dic['id'] == self.args.task):
                self.tasks_config.append(dic)

        self.initialize_tasks()
        self.invoke_tasks()
        msg = 'Cost: %s seconds' % (time.time() - tick)
        self.err_logger.info(msg)

    def _lookup_config(self, tasks_config):
        if self.args.list:
            if self.args.list == 'all':
                tasks_config = sorted(tasks_config, cmp=lambda d1, d2: d1['id'] > d2['id'])
                print '\n'.join(map(lambda task: "Task<id={id}, group={group}>".format(**task), tasks_config))
            else:
                tasks_config = filter(lambda dic: dic['id'] == self.args.list, tasks_config)
                print self.args.list

                if tasks_config:
                    import json
                    print json.dumps(tasks_config[0], indent=4, ensure_ascii=False)
                else:
                    SystemExit("No id '%s'" % self.args.list)
            raise SystemExit
        if self.args.listgroup:
            if self.args.listgroup == 'all':
                groups = set([task['group'] for task in tasks_config])
                print 'groups name:', groups
            else:
                tasks_config = filter(lambda dic: dic['group'] == self.args.listgroup, tasks_config)
                if tasks_config:
                    print "group_name=%s, ids=%s" % (self.args.listgroup, [task['id'] for task in tasks_config])
                else:
                    SystemExit("No group '%s'" % self.args.listgroup)
            raise SystemExit

    def invoke_tasks(self):
        for task in tasks:
            task.start()
            self.err_logger.debug('Invoke Task<id={id}, group={group}> '.format(**task.config))
            if not self.job_config['parallel_tasks']:
                task.join()

        for task in tasks:
            if self.job_config['parallel_tasks']:
                task.join()

    def initialize_tasks(self):
        for task_config in self.tasks_config:
            if 'monitor' in self.job_config:
                monitor_handler_class = import_string(self.job_config['monitor']['handler_class'])
                monitor_handler = monitor_handler_class()
            else:
                monitor_handler = None
            extraction_class = import_string(task_config['extraction']['extraction_class'])
            extractor = extraction_class()
            transformation_class = import_string(task_config['transformation']['transformation_class'])
            transformer = transformation_class()
            loading_class = import_string(task_config['loading']['loading_class'])
            loader = loading_class()

            task_class = import_string(task_config['task_class'])
            task = task_class(self, task_config, extractor, transformer, loader, monitor_handler)
            tasks.append(task)
            msg = 'Initialize {task_class}<id={id}, group={group}> '.format(**task_config)
            self.err_logger.debug(msg)


class DBDispatcher(Dispatcher):
    err_logger_filename = 'dbrp'

    def initialize_tasks(self):
        signal.signal(signal.SIGINT, self._terminate)
        super(DBDispatcher, self).initialize_tasks()

    @staticmethod
    def _terminate(_, __):
        for task in tasks:
            if task.is_alive():
                msg = "Abnormal Termination pid '{pid}'. Task<id={id}, group={group}>".format(pid=task.pid, **task.config)
                DBDispatcher.err_logger.error(msg)
                try:
                    task.terminate()
                except:
                    pass
        raise SystemExit('Keyboard Interrupt')