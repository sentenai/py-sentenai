
test:
	py.test --cov-config=.coveragerc --cov=sentenai --cov-report=term

test-html:
	py.test --cov-config=.coveragerc --cov=sentenai --cov-report=html
