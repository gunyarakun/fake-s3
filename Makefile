.PHONY: build test

build:
	docker build -t gunyarakun/fake-s3 .

test:
	docker run --rm gunyarakun/fake-s3 pytest -v
