import time

import redis
import structlog


def test_persistent_connection(endpoint):
    try:
        r = redis.StrictRedis(host=endpoint, port=6379, socket_keepalive=900)
        elapsed_time = 0
        while elapsed_time < 3600:
            logger.info(f"elapsed time: {elapsed_time}")
            try:
                r.set("a", "a", ex=600)
                r.get("a")

                time.sleep(1)
                elapsed_time += 1
                logger.info("all actions completed.")

            except Exception as e:
                logger.warning(f"{e}. Retrying in 10 seconds...")
                time.sleep(10)

    except Exception as e:
        logger.critical(e)


if __name__ == "__main__":
    logger = structlog.get_logger()
    redis_endpoint = input("Enter the redis endpoint: ")

    logger.debug(f"endpoint in use: {redis_endpoint}")

    test_persistent_connection(redis_endpoint)
