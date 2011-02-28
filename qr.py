"""
QR | Redis-Based Data Structures in Python

    16 Dec 2010 | Add more logging (0.2.1)
    26 Apr 2010 | Major API change; capped collections, remove autopop (0.2.0)
     7 Mar 2010 | Auto popping for bounded queues is optional (0.1.4)
     5 Mar 2010 | Returns work for both bounded and unbounded (0.1.3)
     4 Mar 2010 | Pop commands now return just the value
    24 Feb 2010 | QR now has deque and stack data structures (0.1.2)
    22 Feb 2010 | First public release of QR (0.1.1)
"""

__author__ = 'Ted Nyman'
__version__ = '0.2.1'
__license__ = 'MIT'

import redis
import logging

try:
    import json
except ImportError:
    import simplejson as json


class NullHandler(logging.Handler):
    """A logging handler that discards all logging records"""
    def emit(self, record):
        pass

# Clients can add handlers if they are interested.
log = logging.getLogger('qr')
log.addHandler(NullHandler())


class UsingRedisMixin(object):
    """Hold a reference to redis. Let user pass initialization params to Redis"""
    def __init__(self, redis_init_kwargs = None):
        self.redis = redis.Redis(**(redis_init_kwargs or {}))

	
class Deque(UsingRedisMixin):
    """Implements a double-ended queue"""

    def __init__(self, key, redis_init_kwargs = None):
        self.key = key
        super(Deque, self).__init__(redis_init_kwargs)

    def pushback(self, element):
        """Push an element to the back"""
        key = self.key
        push_it = self.redis.lpush(key, element)
        log.debug('Pushed ** %s ** for key ** %s **' % (element, key))
		
    def pushfront(self, element):
        """Push an element to the front"""
        key = self.key
        push_it = self.redis.rpush(key, element)
        log.debug('Pushed ** %s ** for key ** %s **' % (element, key))

    def popfront(self):
        """Pop an element from the front"""
        key = self.key
        popped = self.redis.rpop(key)
        log.debug('Popped ** %s ** from key ** %s **' % (popped, key))
        return popped 

    def popback(self):
        """Pop an element from the back"""
        key = self.key
        popped = self.redis.lpop(key)
        log.debug('Popped ** %s ** from key ** %s **' % (popped, key))
        return popped 

    def elements(self):
        """Return all elements as a Python list"""
        key = self.key
        all_elements = self.redis.lrange(key, 0, -1)
        return all_elements
				
    def elements_as_json(self):
        """Return all elements as a JSON object"""
        key = self.key
        all_elements = self.redis.lrange(key, 0, -1)
        all_elements_as_json = json.dumps(all_elements)
        return all_elements_as_json

class Queue(UsingRedisMixin):	
    """Implements a FIFO queue"""

    def __init__(self, key, redis_init_kwargs = None):
        self.key = key
        super(Queue, self).__init__(redis_init_kwargs)
	
    def push(self, element):
        """Push an element"""
        key = self.key
        push_it = self.redis.lpush(key, element)
        log.debug('Pushed ** %s ** for key ** %s **' % (element, key))

    def pop(self):
        """Pop an element"""
        key = self.key
        popped = self.redis.rpop(key)
        log.debug('Popped ** %s ** from key ** %s **' % (popped, key))
        return popped 	

    def elements(self):
        """Return all elements as a Python list"""
        key = self.key
        all_elements = self.redis.lrange(key, 0, -1) or [ ]
        return all_elements
				
    def elements_as_json(self):
        """Return all elements as a JSON object"""	
        key = self.key
        all_elements = self.redis.lrange(key, 0, -1) or [ ]
        all_elements_as_json = json.dumps(all_elements)
        return all_elements_as_json

class CappedCollection(UsingRedisMixin):
    """Implements a capped collection (the collection never
    gets larger than the specified size)."""

    def __init__(self, key, size, redis_init_kwargs = None):
        self.key = key
        self.size = size
        super(CappedCollection, self).__init__(redis_init_kwargs)

    def push(self, element):
        key = self.key
        size = self.size
        pipe = self.redis.pipeline() # Use multi-exec command via redis-py pipelining
        pipe = pipe.lpush(key, element).ltrim(key, 0, size-1) # ltrim is zero-indexed 
        pipe.execute()

    def pop(self):
        key = self.key
        popped = self.redis.rpop(key)
        log.debug('Popped ** %s ** from key ** %s **' % (popped, key))
        return popped

    def elements(self):
        """Return all elements as Python list"""
        key = self.key
        all_elements = self.redis.lrange(key, 0, -1)   
        return all_elements

    def elements_as_json(self):
        """Return all elements as JSON object"""
        key = self.key
        all_elements = self.redis.lrange(key, 0, -1)
        all_elements_as_json = json.dumps(all_elements)
        return all_elements_as_json

class Stack(UsingRedisMixin):
    """Implements a LIFO stack""" 

    def __init__(self, key, redis_init_kwargs = None):
        self.key = key
        super(Stack, self).__init__(redis_init_kwargs)

    def push(self, element):
        """Push an element"""
        key = self.key
        push_it = self.redis.lpush(key, element)
        log.debug('Pushed ** %s ** for key ** %s **' % (element, key))
		 
    def pop(self):
        """Pop an element"""
        key = self.key
        popped = self.redis.lpop(key)
        log.debug('Popped ** %s ** from key ** %s **' % (popped, key))
        return popped 
	
    def elements(self):
        """Return all elements as Python list"""
        key = self.key
        all_elements = self.redis.lrange(key, 0, -1)   
        return all_elements

    def elements_as_json(self):
        """Return all elements as JSON object"""
        key = self.key
        all_elements = self.redis.lrange(key, 0, -1)
        all_elements_as_json = json.dumps(all_elements)
        return all_elements_as_json
