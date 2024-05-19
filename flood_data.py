import redis
import structlog

logger = structlog.get_logger()
r = redis.StrictRedis(host="10.0.0.66", port=6379, decode_responses=True)

logger.info(f"{r.ping()}")
