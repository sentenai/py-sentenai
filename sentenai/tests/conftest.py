import pytest
import sentenai

def pytest_addoption(parser):
    parser.addoption("--host", action="store", default="http://localhost:8000", help="add host for testing")
    parser.addoption("--auth", action="store", default="", help="auth: add auth key for testing")


@pytest.fixture(scope="module")
def client(request):
    host = request.config.getoption("--host")
    auth = request.config.getoption("--auth")
    return sentenai.Client(auth_key=auth, host=host)
