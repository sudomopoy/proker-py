import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RetryPolicy:
    def __init__(self, max_retries: int = 3, backoff_factor: float = 1.0):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor


def auto_reconnect(func):
    def wrapper(self, *args, **kwargs):
        retries = 0
        max_retries = self.retry_policy.max_retries
        backoff = self.retry_policy.backoff_factor

        while retries <= max_retries:
            try:
                if not self.is_connected():
                    self.connect()
                return func(self, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error: {str(e)}. Attempt {retries}/{max_retries}")
                retries += 1
                time.sleep(backoff * (2**retries))
        raise ConnectionError("Max retries reached")

    return wrapper
