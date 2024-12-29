class ValidationError(Exception):
    pass


class RetryableError(Exception):
    def __init__(self, message: str, status_code: int):
        self.status_code = status_code
        super().__init__(message)


class MaxRetriesExceededError(Exception):
    pass
