__all__ = [
    'FlareSyntaxError',
    'status_codes',
    'NotFound',
    'AuthenticationError',
    'SentenaiException'
]


class SentenaiException(Exception):
    """Base class for Sentenai expections."""

    pass


class FlareSyntaxError(SentenaiException):
    """A Flare Syntax Error exception."""

    pass


class AuthenticationError(SentenaiException):
    """An Authentication Error exception."""

    pass


class NotFound(SentenaiException):
    """A NotFount Exeption."""

    pass


def status_codes(code):
    """Throw the proper exception depending on the status code."""
    if code == 401:
        raise AuthenticationError("Invalid API key")
    elif code >= 500:
        raise SentenaiException("Something went wrong")
    elif code == 400:
        raise FlareSyntaxError()
    elif code == 404:
        raise NotFound()

    elif code >= 400:
        raise SentenaiException("something went wrong")
