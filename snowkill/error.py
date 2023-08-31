class SnowKillError(Exception):
    pass


class SnowKillRestApiError(SnowKillError):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return f"[{self.code}]: {self.message}"
