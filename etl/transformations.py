class Transformation(object):
    """ This class transform tables."""

    def transform(self, task, job_config, table):
        raise NotImplementedError('method `transform()` must be implemented')

    def get_report(self, task, job_config):
        return {}


class NoneTransformation(Transformation):
    def transform(self, task, job_config, table):
        task.logger.info("%s<>" % self.__class__.__name__)
        return table