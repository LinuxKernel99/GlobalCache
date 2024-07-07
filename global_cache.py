import arrow
import threading
from time import sleep
import concurrent.futures
from typing import Dict, Any, Callable
from pydantic import BaseModel as BM


class AttributeDoesNotExist(Exception):
    ...

class ProviderFailedToExecute(Exception):
    ...

class AttributeAlreadyExist(Exception):
    ...

class CacheMetadata(BM):
    provider: Callable
    refresh_rate: int
    lock: Any
    last_refresh: int | None = None
    last_error: Any = None

    def is_expired(self):
        if self.last_refresh is None:
            return True

        if self.refresh_rate is None:
            return False;

        seconds_passed = (arrow.get() - self.last_refresh).total_seconds()
        return seconds_passed > self.refresh_rate

    def update_last_refresh(self):
        self.last_refresh = arrow.get()

class GlobalCache:
    cache: Dict[str, Any]
    cache_metadata: Dict[str, CacheMetadata]
    max_thread_number: int
    thread_sleep_seconds: int
    register_lock: Any
    excutor: Any
    stop_event: Any

    def __init__(self, max_thread_number: int, thread_sleep_seconds: int = 1):
        self.cache = {}
        self.cache_metadata = {}
        self.max_thread_number = max_thread_number
        self.thread_sleep_seconds = thread_sleep_seconds
        self.register_lock = threading.Lock()
        self.excutor = concurrent.futures.ThreadPoolExecutor(max_workers=max_thread_number)
        self.stop_event = threading.Event()

    def __register(self, attribute: str, provider: Callable, refresh_rate):
        with self.register_lock:
            metadata = CacheMetadata(
                    provider=provider,
                    refresh_rate=refresh_rate,
                    lock = threading.Lock()
                    )

            self.cache[attribute] = None
            self.cache_metadata[attribute] = metadata

    def __refresh(self, attribute):
        try:
            value = self.cache_metadata[attribute].provider()
            self.cache[attribute] = value
            self.cache_metadata[attribute].update_last_refresh()
        except Exception as e:
            raise ProviderFailedToExecute(str(e))

    def __run(self):
        while not self.stop_event.is_set():
            for attribute, metadata in self.cache_metadata.items():
                if metadata.is_expired():
                    self.refresh(attribute)
            sleep(self.thread_sleep_seconds)

    def register(self, attribute: str, provider: Callable, refresh_rate = 0):
        if attribute in self.cache:
            raise AttributeAlreadyExist

        self.__register(attribute, provider, refresh_rate)

    def get(self, attribute: str):
        if attribute not in self.cache:
            raise AttributeDoesNotExist

        return self.cache[attribute]

    def refresh(self, attribute: str):
        if attribute not in self.cache:
            raise AttributeDoesNotExist

        acquired = self.cache_metadata[attribute].lock.acquire(blocking=False)
        if not acquired:
            return
        
        try:
            if self.cache_metadata[attribute].provider is not None:
                self.__refresh(attribute)
        except ProviderFailedToExecute as ex:
            self.cache_metadata[attribute].last_error = ex
        finally:
            self.cache_metadata[attribute].lock.release()
        
    def get_error(self, attribute):
        return self.cache_metadata[attribute].last_error

    def run(self):
        for _ in range(self.max_thread_number):
            self.excutor.submit(self.__run)

    def stop(self):
        self.stop_event.set()
        self.excutor.shutdown(wait=True)
        self.stop_event.clear()

