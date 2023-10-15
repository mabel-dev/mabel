lint:
	python -m pip install --quiet --upgrade pycln isort black
	python -m pycln .
	python -m isort .
	python -m black .

update:
	python -m pip install --upgrade pip
	python -m pip install --upgrade -r requirements.txt
	python -m pip install --upgrade -r tests/requirements.txt

test:
	clear
	python -m pytest

coverage:
	python -m coverage run -m pytest 
	python -m coverage report --include=mabel/** -m