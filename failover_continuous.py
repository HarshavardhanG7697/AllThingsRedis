import time

import redis
import structlog


def test_continuous_connection(endpoint):
    elapsed_time = 0
    while elapsed_time < 3600:
        try:
            logger.info(f"elapsed time = {elapsed_time}")
            # Attempt to connect to ElastiCache cluster
            r = redis.Redis(host=endpoint, port=6379)

            # If successful, perform a test operation (e.g., ping)
            r.ping()
            logger.info(f"new connection to {endpoint}")

            time.sleep(1)
            elapsed_time += 1

        except redis.RedisError as e:
            logger.warning(f"{e}. Retrying in 10 seconds...")
            time.sleep(10)


if __name__ == "__main__":
    logger = structlog.get_logger()
    redis_endpoint = input("Enter the redis endpoint: ")

    test_continuous_connection(redis_endpoint)
