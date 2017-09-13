setup:
	@pip install -r development.txt

test:
	@pytest --cov=json2parquet tests/

clean:
	@rm -rf .cache
	@rm -rf tests/__pycache__
	@rm -rf build/

