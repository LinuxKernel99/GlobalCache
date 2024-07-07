import arrow
from time import sleep
from random import randint
from global_cache import GlobalCache

cache = GlobalCache(3)

def get():
    if cache.get_error('first_key') is None:
        raise Exception('fuck')
    sleep(1)
    return arrow.get() 

def get_data():
    sleep(10)
    return [randint(1, 100), arrow.get()]

def get_data_slow():
    sleep(10)
    return 'love'

cache.register('first_key', get, 1)
cache.register('data', get_data, 1)
cache.register('slow_data', get_data_slow, 1)
cache.run()
print(cache.get_error('first_key'))

while True:
    print(cache.get('first_key'))
    print(cache.get('data'))
    print(cache.get('slow_data'))
    sleep(1)
