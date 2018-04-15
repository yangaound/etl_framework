
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
