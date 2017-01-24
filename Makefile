all: carbonzipper

GO ?= go
export GOPATH := $(CURDIR)/_vendor

carbonzipper:
	$(GO) build

tmp/carbonzipper.tar.gz: carbonzipper
	mkdir -p tmp/
	rm -rf tmp/carbonzipper
	mkdir -p tmp/carbonzipper/
	cp carbonzipper tmp/carbonzipper/carbonzipper
	cp example.conf tmp/carbonzipper/carbonzipper.conf
	cp deploy/carbonzipper.init.centos tmp/carbonzipper/carbonzipper.init
	cd tmp && tar czf carbonzipper.tar.gz carbonzipper/

rpm: tmp/carbonzipper.tar.gz
	cp deploy/buildrpm.sh tmp/buildrpm.sh
	cd tmp && ./buildrpm.sh ../deploy/carbonzipper.spec.centos `../carbonzipper --version`

submodules:
	git submodule init
	git submodule update --recursive

clean:
	rm -f carbonzipper

image:
	CGO_ENABLED=0 GOOS=linux $(MAKE) carbonzipper
	docker build -t carbonzipper .
