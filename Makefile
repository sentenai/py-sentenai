
test:
	py.test --cov-config=.coveragerc --cov=sentenai --cov-report=term tests/

test-html:
	py.test --cov-config=.coveragerc --cov=sentenai --cov-report=html tests/
