
class Writing(object):
    """ Abstract class that load data from a given table into a file-like destination or database."""

    def setup(self, task, job_config):
        """prepare environment/runtime related dependencies"""

    def teardown(self, task, job_config):
        """recovery environment or free runtime related resource"""

    def load(self, task, job_config, table):
        """loading the table"""
        raise NotImplementedError('method `load()` must be implemented')

    def get_report(self, task, job_config):
        return {}
