import pytest
from time import sleep
from global_cache import GlobalCache

@pytest.fixture
def cache():
    instance = GlobalCache(3, 0.1)
    instance.run()
    yield instance
    instance.stop()

def get(hmmm):
    sleep(3)
    return hmmm 

def get_data():
    sleep(10)
    return 2

def get_data_slow():
    sleep(5)
    return 3

def test_a(cache):
    value = 99
    cache.register('first_key', get, (value, ), 1)
    cache.register('data', get_data, refresh_rate = 1)
    cache.register('slow_data', get_data_slow, refresh_rate = 1)
    assert cache.get('first_key') is None
    assert cache.get('data') is None
    assert cache.get('slow_data') is None
    sleep(12)
    assert cache.get('first_key') == value
    assert cache.get('data') == 2
    assert cache.get('slow_data') == 3

def test_b(cache):
    value = 100
    cache.register('something', get, (value, ), 5)
    assert cache.get('something') is None 
    sleep(5)
    assert cache.get('something') == value
