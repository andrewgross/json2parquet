setup:
	@pip install -r development.txt

test:
	@pytest

clean:
	@rm -rf .cache
	@rm -rf tests/__pycache__
	@rm -rf build/

