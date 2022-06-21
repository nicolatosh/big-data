from select import select
import redis

class RedisManager():
    """
    Wrapper for Redis client
    """

    def __init__(self, host="127.0.0.1", port="6379", password="redis_password") -> None:
        self.__instance = redis.Redis(
                            host=host,
                            port=port,
                            password=password
                            )

    def get_instance(self):
        return self.__instance

    def test_connection(self) -> bool:
        return self.__instance.ping()

