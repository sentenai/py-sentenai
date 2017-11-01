__all__ = ['FlareSyntaxError', 'status_codes', 'NotFound', 'AuthenticationError', 'SentenaiException']

class SentenaiException(Exception):
    pass

class APIError(SentenaiException):
    def __init__(self, resp):
        self.response = resp

class FlareSyntaxError(SentenaiException):
    pass

class AuthenticationError(SentenaiException):
    pass

class NotFound(SentenaiException):
    pass

def status_codes(resp):
    code = resp.status_code
    if code == 401:
        raise AuthenticationError("Invalid API key")
    elif code >= 500:
        raise SentenaiException("Something went wrong")
    elif code == 400:
        raise FlareSyntaxError()
    elif code == 404:
        raise NotFound()

    elif code >= 400:
        raise APIError(resp)

def handle(resp):
    # handle bad status codes
    if resp.status_code == 401:
        raise AuthenticationError("Invalid API Key")
    elif resp.status_code == 400:
        raise FlareSyntaxError
    elif resp.status_code >= 500:
        raise SentenaiException("Something went wrong.")
    elif resp.status_code < 200 or resp.status_code >= 400:
        raise SentenaiException("Something went wrong. Code: %i" % resp.status_code)
    return resp
