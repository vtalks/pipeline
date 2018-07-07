VERSION=`cat VERSION`

# Docker container images

.PHONY: docker
docker: docker-build docker-publish

.PHONY: docker-build
docker-build:	## Builds container and tag resulting image
	docker build --force-rm --tag vtalks/pipeline .
	docker tag vtalks/pipeline vtalks/pipeline:$(VERSION)

.PHONY: docker-publish
docker-publish:	## Publishes container images
	docker push vtalks/pipeline:$(VERSION)
	docker push vtalks/pipeline:latest

include Makefile.help.mk
