from typing import Callable, Any
import time
import logging
from src.api.utils.exceptions import MaxRetriesExceededError, RetryableError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RetryHandler:
    def __init__(
        self,
        max_retries: int = 5,
        retry_delay: int = 300,  # 5 minutes in seconds
        status_codes_to_retry: set = {429},
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.status_codes_to_retry = status_codes_to_retry

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        attempts = 0

        while attempts < self.max_retries:
            try:
                response = func(*args, **kwargs)
                if hasattr(response, "status") and response.status == 429:
                    raise RetryableError("Rate limit exceeded", status_code=429)
                return response

            except RetryableError as e:
                attempts += 1
                if attempts == self.max_retries:
                    raise MaxRetriesExceededError(
                        f"Max retries ({self.max_retries}) exceeded"
                    )

                logger.warning(
                    f"Attempt {attempts}/{self.max_retries} failed with status {e.status_code}. "
                    f"Retrying in {self.retry_delay} seconds..."
                )
                time.sleep(self.retry_delay)
