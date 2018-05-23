__all__ = [
    'QuerySyntaxError',
    'status_codes',
    'NotFound',
    'AuthenticationError',
    'SentenaiException'
]


class SentenaiException(Exception):
    """Base class for Sentenai expections."""
    pass

class APIError(SentenaiException):
    def __init__(self, resp):
        self.response = resp

class QuerySyntaxError(SentenaiException):
    """A Query Syntax Error exception."""
    pass


class AuthenticationError(SentenaiException):
    """An Authentication Error exception."""
    pass


class NotFound(SentenaiException):
    """A NotFount Exeption."""
    pass

def status_codes(resp):
    """Throw the proper exception depending on the status code."""

    code = resp.status_code
    if code == 401:
        raise AuthenticationError("Invalid API key")
    elif code >= 500:
        raise SentenaiException("Something went wrong")
    elif code == 400:
        raise QuerySyntaxError()
    elif code == 404:
        raise NotFound()
    elif code >= 400:
        raise APIError(resp)

def handle(resp):
    """Handle bad status codes"""

    if resp.status_code == 401:
        raise AuthenticationError("Invalid API Key")
    elif resp.status_code == 400:
        raise QuerySyntaxError
    elif resp.status_code < 200 or resp.status_code >= 400:
        raise SentenaiException("Something went wrong. Code: %i" % resp.status_code)
    return resp
