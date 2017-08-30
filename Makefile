
test:
	python3 -m unittest discover -s kiwiii.test

builddocs:
	cd docs && make html
