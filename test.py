import arrow
import pytest
from time import sleep
from random import randint
from global_cache import GlobalCache

@pytest.fixture
def cache():
    instance = GlobalCache(3)
    instance.run()
    yield instance
    instance.stop()

def get():
    sleep(3)
    return arrow.get() 

def get_data():
    sleep(10)
    return [randint(1, 100), arrow.get()]

def get_data_slow():
    sleep(5)
    return 'love'


def test_a(cache):
    cache.register('first_key', get, 1)
    cache.register('data', get_data, 1)
    cache.register('slow_data', get_data_slow, 1)
    sleep(12)
    print(cache.get('first_key'))
    print(cache.get('data'))
    print(cache.get('slow_data'))

def test_b(cache):
    cache.register('something', get, 5)
    sleep(5)
    print(cache.get('something'))
