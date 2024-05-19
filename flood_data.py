import random
import string
import time

import redis
import structlog

logger = structlog.get_logger()

redis_host = input("Enter redis host: ")
logger.debug(f"redis host is set to: {redis_host}")


def generate_random_string(size_mb=1):
    """Generates a random string of approximately the specified size (in MB)."""
    chars = (
        string.ascii_letters + string.digits + string.punctuation
    )  # All printable characters
    chars_per_mb = 1024 * 1024  # Assuming 1 byte per character (rough approximation)
    size_chars = size_mb * chars_per_mb
    return "".join(random.choice(chars) for _ in range(size_chars))


def retry_set_with_backoff(client, key, value, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            client.set(key, value)  # Set with 5-minute expiration
            logger.info(f"Successfully set key: {key}")
            return  # Success, exit the retry loop
        except redis.exceptions.OutOfMemoryError as e:
            sleep_time = random.uniform(1, 3)  # Random sleep between 1 and 3 seconds
            logger.warning(
                f"Memory limit reached. Retrying in {sleep_time:.2f} seconds..."
            )
            time.sleep(sleep_time)
            retries += 1
        except Exception as e:
            raise e

    logger.error(f"Failed to set key {key} after {max_retries} retries.")


def read_key(client, key, max_retries=3):
    retries = 0
    while retries < max_retries:
        response = client.get(key)

        if not response:
            sleep_time = random.uniform(1, 2)
            logger.warning(f"unable to locate key: {key}, retrying in {sleep_time:.2f}")
            time.sleep(sleep_time)
            retries += 1
        else:
            logger.info(f"located key: {key}.")
            return
    logger.error(f"unable to locate key: {key} even after {max_retries} retries.")


read_or_write = str(input("Perform Write load or Read load?:\n")).lower()

if read_or_write == "write":
    try:
        r = redis.StrictRedis(host=redis_host, port=6379, decode_responses=True)
        logger.debug("Connection with the Redis host is established.")

        for i in range(1000):
            retry_set_with_backoff(r, i, generate_random_string(size_mb=10))

    except Exception as e:
        logger.critical(e)

elif read_or_write == "read":
    try:
        r = redis.StrictRedis(host=redis_host, port=6379, decode_responses=True)
        logger.debug("Connection with the Redis host is established.")

        while True:
            for i in range(10):
                read_key(r, i, max_retries=2)

    except Exception as e:
        logger.critical(e)

else:
    logger.error("choose between 'read' and 'write' only. try again.")
