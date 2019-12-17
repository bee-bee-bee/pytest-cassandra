# -*- coding: utf-8 -*-

import pytest
from configparser import ConfigParser
from pytest_cassandra.db import DB


def pytest_addoption(parser):
    group = parser.getgroup('pytest_cassandra')
    group.addoption(
        '--config_cassandra',
        action='store',
        # default='config/config.yml',
        help='relative path of config.yml'
    )

    # parser.addini('HELLO', 'Dummy pytest.ini setting')


@pytest.fixture(scope="session", autouse=False)
def cassandracmdopt(request):
    option_config = request.config.getoption("--config_cassandra")
    if option_config:
        return option_config
    else:
        try:
            ini_config = request.config.inifile.strpath
            config = ConfigParser()
            config.read(ini_config)
            cassandra_config = config.get('cassandra', 'config')
            return cassandra_config
        except Exception as e:
            raise RuntimeError("there is no cassandra config in pytest.ini", e)


@pytest.fixture(scope="session", autouse=False)
def cassandra(cassandracmdopt, request):
    return DB(cassandracmdopt, request.config.rootdir).cassandra
