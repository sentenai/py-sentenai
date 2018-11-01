
test:
	PYTHONPATH=. python -m pytest --cov-config=.coveragerc --cov=sentenai --cov-report=term tests/

test-html:
	PYTHONPATH=. python -m pytest --cov-config=.coveragerc --cov=sentenai --cov-report=html tests/
