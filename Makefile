.PHONY: build test shell

build:
	docker build -t gunyarakun/fake-s3 .

test: build
	docker run --rm gunyarakun/fake-s3 pytest -v

shell: build
	docker run --rm -it -v $(PWD)/fake-s3:/app gunyarakun/fake-s3 bash
