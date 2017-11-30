
test:
	PYTHONPATH=. py.test --cov-config=.coveragerc --cov=sentenai --cov-report=term tests/

test-html:
	PYTHONPATH=. py.test --cov-config=.coveragerc --cov=sentenai --cov-report=html tests/
