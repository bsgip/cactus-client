class CactusClientException(Exception):
    """General base exception for anything the CactusClient might raise"""

    pass


class ConfigException(CactusClientException):
    """Something is wrong/missing with the current Cactus Client configuration"""

    pass


class RequestException(CactusClientException):
    """Something is went wrong when accessing the remote CSIP-Aus utility server"""

    pass
