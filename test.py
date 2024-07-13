import arrow
import pytest
from time import sleep
from global_cache import GlobalCache

@pytest.fixture
def cache():
    instance = GlobalCache(3, 0.1)
    instance.run()
    yield instance
    instance.stop()

def get():
    sleep(3)
    return 1 

def get_data():
    sleep(10)
    return 2

def get_data_slow():
    sleep(5)
    return 3


def test_a(cache):
    cache.register('first_key', get, 1)
    cache.register('data', get_data, 1)
    cache.register('slow_data', get_data_slow, 1)
    assert cache.get('first_key') is None
    assert cache.get('data') is None
    assert cache.get('slow_data') is None
    sleep(12)
    assert cache.get('first_key') == 1
    assert cache.get('data') == 2
    assert cache.get('slow_data') == 3

def test_b(cache):
    cache.register('something', get, 5)
    assert cache.get('something') is None 
    sleep(5)
    assert cache.get('something') == 1
