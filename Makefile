
test:
	python3 -m unittest discover -s flashflood.test

builddocs:
	cd docs && make html
