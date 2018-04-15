import petl


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


class NoneExtraction(Extraction):
    def extract(self, task, job_config):
        return petl.empty()
