"""
Threadsafe pure-Python implementation of the Circuit Breaker pattern, described
"""

import types
import time
import calendar
import logging
from datetime import datetime, timedelta
from functools import wraps
import threading
import six
import sys

try:
    from tornado import gen
    HAS_TORNADO_SUPPORT = True
except ImportError:
    HAS_TORNADO_SUPPORT = False

try:
    from redis.exceptions import RedisError
    HAS_REDIS_SUPPORT = True
except ImportError:
    HAS_REDIS_SUPPORT = False

__all__ = (
    'CircuitBreaker', 'CircuitBreakerListener', 'CircuitBreakerError',
    'CircuitMemoryStorage', 'CircuitRedisStorage', 'STATE_OPEN', 'STATE_CLOSED',
    'STATE_HALF_OPEN',)

STATE_OPEN = 'open'
STATE_CLOSED = 'closed'
STATE_HALF_OPEN = 'half-open'


class CircuitBreaker(object):

    def __init__(self, fail_max=5, reset_timeout=60, exclude=None,
                 listeners=None, state_storage=None, name=None):

        self._lock = threading.RLock()
        self._state_storage = state_storage or CircuitMemoryStorage(STATE_CLOSED)
        self._state = self._create_new_state(self.current_state)

        self._fail_max = fail_max
        self._reset_timeout = reset_timeout

        self._excluded_exceptions = list(exclude or [])
        self._listeners = list(listeners or [])
        self._name = name

    @property
    def fail_counter(self):
        return self._state_storage.counter

    @property
    def fail_max(self):
        return self._fail_max

    @fail_max.setter
    def fail_max(self, number):
        self._fail_max = number

    @property
    def reset_timeout(self):
        return self._reset_timeout

    @reset_timeout.setter
    def reset_timeout(self, timeout):
        self._reset_timeout = timeout

    def _create_new_state(self, new_state, prev_state=None, notify=False):
        state_map = {
            STATE_CLOSED: CircuitClosedState,
            STATE_OPEN: CircuitOpenState,
            STATE_HALF_OPEN: CircuitHalfOpenState,
        }
        try:
            cls = state_map[new_state]
            return cls(self, prev_state=prev_state, notify=notify)
        except KeyError:
            msg = "Unknown state {!r}, valid states: {}"
            raise ValueError(msg.format(new_state, ', '.join(state_map)))

    @property
    def state(self):

        if self.current_state != self._state.name:
            self.state = self.current_state
        return self._state

    @state.setter
    def state(self, state_str):
        with self._lock:
            self._state = self._create_new_state(
                state_str, prev_state=self._state, notify=True)

    @property
    def current_state(self):
        return self._state_storage.state

    @property
    def excluded_exceptions(self):
        return tuple(self._excluded_exceptions)

    def add_excluded_exception(self, exception):
        with self._lock:
            self._excluded_exceptions.append(exception)

    def add_excluded_exceptions(self, *exceptions):
        for exc in exceptions:
            self.add_excluded_exception(exc)

    def remove_excluded_exception(self, exception):
        with self._lock:
            self._excluded_exceptions.remove(exception)

    def _inc_counter(self):
        self._state_storage.increment_counter()

    def is_system_error(self, exception):
        exception_type = type(exception)
        for exclusion in self._excluded_exceptions:
            if type(exclusion) is type:
                if issubclass(exception_type, exclusion):
                    return False
            elif callable(exclusion):
                if exclusion(exception):
                    return False
        return True

    def call(self, func, *args, **kwargs):
        with self._lock:
            return self.state.call(func, *args, **kwargs)

    def call_async(self, func, *args, **kwargs):
        @gen.coroutine
        def wrapped():
            with self._lock:
                ret = yield self.state.call_async(func, *args, **kwargs)
                raise gen.Return(ret)
        return wrapped()

    def open(self):
        with self._lock:
            self._state_storage.opened_at = datetime.utcnow()
            self.state = self._state_storage.state = STATE_OPEN

    def half_open(self):
        with self._lock:
            self.state = self._state_storage.state = STATE_HALF_OPEN

    def close(self):
        with self._lock:
            self.state = self._state_storage.state = STATE_CLOSED

    def __call__(self, *call_args, **call_kwargs):
        call_async = call_kwargs.pop('__pybreaker_call_async', False)

        if call_async and not HAS_TORNADO_SUPPORT:
            raise ImportError('No module named tornado')

        def _outer_wrapper(func):
            @wraps(func)
            def _inner_wrapper(*args, **kwargs):
                if call_async:
                    return self.call_async(func, *args, **kwargs)
                return self.call(func, *args, **kwargs)
            return _inner_wrapper

        if call_args:
            return _outer_wrapper(*call_args)
        return _outer_wrapper

    @property
    def listeners(self):
        return tuple(self._listeners)

    def add_listener(self, listener):
        with self._lock:
            self._listeners.append(listener)

    def add_listeners(self, *listeners):
        for listener in listeners:
            self.add_listener(listener)

    def remove_listener(self, listener):
        with self._lock:
            self._listeners.remove(listener)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        """
        Set the name of this circuit breaker.
        """
        self._name = name


class CircuitBreakerStorage(object):
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def state(self):
        pass

    @state.setter
    def state(self, state):
        pass

    def increment_counter(self):
        pass

    def reset_counter(self):
        pass

    @property
    def counter(self):
        pass

    @property
    def opened_at(self):
        pass

    @opened_at.setter
    def opened_at(self, datetime):
        pass


class CircuitMemoryStorage(CircuitBreakerStorage):
    def __init__(self, state):
        super(CircuitMemoryStorage, self).__init__('memory')
        self._fail_counter = 0
        self._opened_at = None
        self._state = state

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

    def increment_counter(self):
        self._fail_counter += 1

    def reset_counter(self):
        self._fail_counter = 0

    @property
    def counter(self):
        return self._fail_counter

    @property
    def opened_at(self):
        return self._opened_at

    @opened_at.setter
    def opened_at(self, datetime):
        self._opened_at = datetime


class CircuitRedisStorage(CircuitBreakerStorage):
    """
    Implements a `CircuitBreakerStorage` using redis.
    """

    BASE_NAMESPACE = 'pybreaker'

    logger = logging.getLogger(__name__)

    def __init__(self, state, redis_object, namespace=None, fallback_circuit_state=STATE_CLOSED):
        # Module does not exist, so this feature is not available
        if not HAS_REDIS_SUPPORT:
            raise ImportError("CircuitRedisStorage can only be used if the required dependencies exist")

        super(CircuitRedisStorage, self).__init__('redis')

        try:
            self.RedisError = __import__('redis').exceptions.RedisError
        except ImportError:
            # Module does not exist, so this feature is not available
            raise ImportError("CircuitRedisStorage can only be used if 'redis' is available")

        self._redis = redis_object
        self._namespace_name = namespace
        self._fallback_circuit_state = fallback_circuit_state
        self._initial_state = str(state)

        self._initialize_redis_state(self._initial_state)

    def _initialize_redis_state(self, state):
        self._redis.setnx(self._namespace('fail_counter'), 0)
        self._redis.setnx(self._namespace('state'), state)

    @property
    def state(self):
        """
        Returns the current circuit breaker state.
        If the circuit breaker state on Redis is missing, re-initialize it
        with the fallback circuit state and reset the fail counter.
        """
        try:
            state_bytes = self._redis.get(self._namespace('state'))
        except self.RedisError:
            self.logger.error('RedisError: falling back to default circuit state', exc_info=True)
            return self._fallback_circuit_state

        state = self._fallback_circuit_state
        if state_bytes is not None:
            state = state_bytes.decode('utf-8')
        else:
            # state retrieved from redis was missing, so we re-initialize
            # the circuit breaker state on redis
            self._initialize_redis_state(self._fallback_circuit_state)

        return state

    @state.setter
    def state(self, state):
        """
        Set the current circuit breaker state to `state`.
        """
        try:
            self._redis.set(self._namespace('state'), str(state))
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def increment_counter(self):
        """
        Increases the failure counter by one.
        """
        try:
            self._redis.incr(self._namespace('fail_counter'))
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def reset_counter(self):
        """
        Sets the failure counter to zero.
        """
        try:
            self._redis.set(self._namespace('fail_counter'), 0)
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    @property
    def counter(self):
        """
        Returns the current value of the failure counter.
        """
        try:
            value = self._redis.get(self._namespace('fail_counter'))
            if value:
                return int(value)
            else:
                return 0
        except self.RedisError:
            self.logger.error('RedisError: Assuming no errors', exc_info=True)
            return 0

    @property
    def opened_at(self):
        """
        Returns a datetime object of the most recent value of when the circuit
        was opened.
        """
        try:
            timestamp = self._redis.get(self._namespace('opened_at'))
            if timestamp:
                return datetime(*time.gmtime(int(timestamp))[:6])
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            return None

    @opened_at.setter
    def opened_at(self, now):
        """
        Atomically sets the most recent value of when the circuit was opened
        to `now`. Stored in redis as a simple integer of unix epoch time.
        To avoid timezone issues between different systems, the passed in
        datetime should be in UTC.
        """
        try:
            key = self._namespace('opened_at')
            def set_if_greater(pipe):
                current_value = pipe.get(key)
                next_value = int(calendar.timegm(now.timetuple()))
                pipe.multi()
                if not current_value or next_value > int(current_value):
                    pipe.set(key, next_value)

            self._redis.transaction(set_if_greater, key)
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def _namespace(self, key):
        name_parts = [self.BASE_NAMESPACE, key]
        if self._namespace_name:
            name_parts.insert(0, self._namespace_name)

        return ':'.join(name_parts)


class CircuitBreakerListener(object):
    """
    Listener class used to plug code to a ``CircuitBreaker`` instance when
    certain events happen.
    """

    def before_call(self, cb, func, *args, **kwargs):
        """
        This callback function is called before the circuit breaker `cb` calls
        `fn`.
        """
        pass

    def failure(self, cb, exc):
        """
        This callback function is called when a function called by the circuit
        breaker `cb` fails.
        """
        pass

    def success(self, cb):
        """
        This callback function is called when a function called by the circuit
        breaker `cb` succeeds.
        """
        pass

    def state_change(self, cb, old_state, new_state):
        """
        This callback function is called when the state of the circuit breaker
        `cb` state changes.
        """
        pass


class CircuitBreakerState(object):
    """
    Implements the behavior needed by all circuit breaker states.
    """

    def __init__(self, cb, name):
        """
        Creates a new instance associated with the circuit breaker `cb` and
        identified by `name`.
        """
        self._breaker = cb
        self._name = name

    @property
    def name(self):
        """
        Returns a human friendly name that identifies this state.
        """
        return self._name

    def _handle_error(self, exc, reraise=True):
        """
        Handles a failed call to the guarded operation.
        """
        if self._breaker.is_system_error(exc):
            self._breaker._inc_counter()
            for listener in self._breaker.listeners:
                listener.failure(self._breaker, exc)
            self.on_failure(exc)
        else:
            self._handle_success()

        if reraise:
            raise exc

    def _handle_success(self):
        """
        Handles a successful call to the guarded operation.
        """
        self._breaker._state_storage.reset_counter()
        self.on_success()
        for listener in self._breaker.listeners:
            listener.success(self._breaker)

    def call(self, func, *args, **kwargs):
        """
        Calls `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.
        """
        ret = None

        self.before_call(func, *args, **kwargs)
        for listener in self._breaker.listeners:
            listener.before_call(self._breaker, func, *args, **kwargs)

        try:
            ret = func(*args, **kwargs)
            if isinstance(ret, types.GeneratorType):
                return self.generator_call(ret)

        except BaseException as e:
            self._handle_error(e)
        else:
            self._handle_success()
        return ret

    def call_async(self, func, *args, **kwargs):
        """
        Calls async `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.
        Return a closure to prevent import errors when using without tornado present
        """
        @gen.coroutine
        def wrapped():
            ret = None

            self.before_call(func, *args, **kwargs)
            for listener in self._breaker.listeners:
                listener.before_call(self._breaker, func, *args, **kwargs)

            try:
                ret = yield func(*args, **kwargs)
                if isinstance(ret, types.GeneratorType):
                    raise gen.Return(self.generator_call(ret))

            except BaseException as e:
                self._handle_error(e)
            else:
                self._handle_success()
            raise gen.Return(ret)
        return wrapped()

    def generator_call(self, wrapped_generator):
        try:
            value = yield next(wrapped_generator)
            while True:
                value = yield wrapped_generator.send(value)
        except StopIteration:
            self._handle_success()
            return
        except BaseException as e:
            self._handle_error(e, reraise=False)
            wrapped_generator.throw(e)

    def before_call(self, func, *args, **kwargs):
        """
        Override this method to be notified before a call to the guarded
        operation is attempted.
        """
        pass

    def on_success(self):
        """
        Override this method to be notified when a call to the guarded
        operation succeeds.
        """
        pass

    def on_failure(self, exc):
        """
        Override this method to be notified when a call to the guarded
        operation fails.
        """
        pass


class CircuitClosedState(CircuitBreakerState):
    def __init__(self, cb, prev_state=None, notify=False):
        super(CircuitClosedState, self).__init__(cb, STATE_CLOSED)
        if notify:

            self._breaker._state_storage.reset_counter()
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exc):
        if self._breaker._state_storage.counter >= self._breaker.fail_max:
            self._breaker.open()

            error_msg = 'Failures threshold reached, circuit breaker opened'
            six.reraise(CircuitBreakerError, CircuitBreakerError(error_msg), sys.exc_info()[2])


class CircuitOpenState(CircuitBreakerState):
    def __init__(self, cb, prev_state=None, notify=False):
        super(CircuitOpenState, self).__init__(cb, STATE_OPEN)
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def before_call(self, func, *args, **kwargs):
        timeout = timedelta(seconds=self._breaker.reset_timeout)
        opened_at = self._breaker._state_storage.opened_at
        if opened_at and datetime.utcnow() < opened_at + timeout:
            error_msg = 'Timeout not elapsed yet, circuit breaker still open'
            raise CircuitBreakerError(error_msg)
        else:
            self._breaker.half_open()
            return self._breaker.call(func, *args, **kwargs)

    def call(self, func, *args, **kwargs):
        return self.before_call(func, *args, **kwargs)


class CircuitHalfOpenState(CircuitBreakerState):
    def __init__(self, cb, prev_state=None, notify=False):
        super(CircuitHalfOpenState, self).__init__(cb, STATE_HALF_OPEN)
        if notify:
            for listener in self._breaker._listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exc):
        self._breaker.open()
        error_msg = 'Trial call failed, circuit breaker opened'
        six.reraise(CircuitBreakerError, CircuitBreakerError(error_msg), sys.exc_info()[2])

    def on_success(self):
        self._breaker.close()


class CircuitBreakerError(Exception):
    pass