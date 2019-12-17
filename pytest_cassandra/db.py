#!/usr/bin/env python
# coding=utf-8
import yaml
from pytest_cassandra.cassandra_client import CassandraManager
from pytest_cassandra.logger import logger


def singleton(cls):
    def _singleton(*args, **kwargs):
        instance = cls(*args, **kwargs)
        instance.__call__ = lambda: instance
        return instance

    return _singleton


@singleton
class DB(object):
    def __init__(self, cassandracmdopt, rootdir):
        config_path = '{0}/{1}'.format(rootdir, cassandracmdopt)
        with open(config_path) as f:
            self.env = yaml.load(f, Loader=yaml.FullLoader)

    @property
    def cassandra(self):
        cass_dict = dict()
        try:
            for k, v in self.env.get('cassandra', {}).items():
                cass_dict[k] = CassandraManager(**v)
        except Exception as e:
            logger.error(e)
            raise ConnectionError(e)
        return cass_dict
