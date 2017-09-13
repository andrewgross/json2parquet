setup:
	@pip install -r development.txt
	@pre-commit install

pre_commit:
	@pre-commit run --all-files

test:
	@pytest --cov=json2parquet tests/

clean:
	@rm -rf .cache
	@rm -rf tests/__pycache__
	@rm -rf build/
