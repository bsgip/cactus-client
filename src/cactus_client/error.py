class CactusClientError(Exception):
    """General base exception for anything the CactusClient might raise"""

    pass


class ConfigError(CactusClientError):
    """Something is wrong/missing with the current Cactus Client configuration"""

    pass


class RequestError(CactusClientError):
    """Something went wrong when accessing the remote CSIP-Aus utility server"""

    pass


class NotificationError(CactusClientError):
    """Something went wrong when interacting with the remote CACTUS Client Notification server."""

    pass
