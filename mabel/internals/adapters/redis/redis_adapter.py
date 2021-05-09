import os
import redis
import mabel

class RedisAdapter():

    _redis_connection = None

    @staticmethod
    def retrieve_object(object_name: str):

        host = os.environ.get("REDIS_HOST", None)
        if not host:
            return None

        if not RedisAdapter._redis_connection:            
            port = int(os.environ.get("REDIS_PORT", 6379))
            RedisAdapter._redis_connection = redis.Redis(host=host, port=port, db=0)

        return RedisAdapter._redis_connection.get(object_name)

    @staticmethod
    def set_object(object_name: str, value):

        host = os.environ.get("REDIS_HOST", None)
        if not host:
            return False

        if not RedisAdapter._redis_connection:            
            port = int(os.environ.get("REDIS_PORT", 6379))
            RedisAdapter._redis_connection = redis.Redis(host=host, port=port, db=0)

        RedisAdapter._redis_connection.set(object_name, value)

        return True

if __name__ == "__main__":

    print(RedisAdapter().retrieve_object('object'))

    print(RedisAdapter().set_object('object', b'bytes'))