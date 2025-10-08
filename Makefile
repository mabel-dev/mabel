SRC_DIR := mabel
PYTHON := python
UV := $(PYTHON) -m uv
PIP := $(UV) pip

define print_green
	@echo "\033[0;32m$(1)\033[0m"
endef

define print_blue
	@echo "\033[0;34m$(1)\033[0m"
endef

lint: ## Run all linting tools
	$(call print_blue,"Installing linting tools...")
	@$(PIP) install --quiet --upgrade pycln isort ruff
	$(call print_blue,"Cleaning unused imports...")
	@$(PYTHON) -m pycln .
	$(call print_blue,"Sorting imports...")
	@$(PYTHON) -m isort .
	$(call print_blue,"Formatting code...")
	@$(PYTHON) -m ruff format $(SRC_DIR)
	$(call print_green,"Linting complete!")

update: ## Update all dependencies
	$(call print_blue,"Updating dependencies...")
	@$(PYTHON) -m pip install --upgrade pip uv
	@$(UV) pip install --upgrade -r tests/requirements.txt
	@$(UV) pip install --upgrade -r requirements.txt

test:
	clear
	python -m pytest

coverage:
	python -m coverage run -m pytest 
	python -m coverage report --include=mabel/** -m