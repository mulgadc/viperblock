GO_PROJECT_NAME := viperblock

build:
	$(MAKE) go_build

# GO commands
go_build:
	@echo "\n....Building $(GO_PROJECT_NAME)"
	go build -ldflags "-s -w" -o ./bin/sfs cmd/sfs/sfs.go

# Build multi-arch for docker, TODO add ARM
#go_build_docker:
#	@echo "\n....Building $(GO_PROJECT_NAME)"
#	GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" --ldflags '-extldflags "-static"' -o ./bin/linux/s3d cmd/s3d/main.go
#
#	GOOS=darwin GOARCH=$(GOARCH) go build -ldflags "-s -w" -o ./bin/darwin/s3d cmd/s3d/main.go

#go_run:
#	@echo "\n....Running $(GO_PROJECT_NAME)...."
#	$(GOPATH)/bin/$(GO_PROJECT_NAME)

test:
	@echo "\n....Running tests for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -v ./...

bench:
	@echo "\n....Running benchmarks for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -benchmem -run=. -bench=. ./...

#dev:
#	air go run cmd/vbd/main.go

# Docker builds
#docker_s3d:
#	@echo "Building docker (vbd)"
#	docker build -t mulgadc/viperblock:latest -f- . < docker/Dockerfile-vbd

#docker_compose_up:
#	@echo "Running docker-compose"
#	docker-compose -f docker/docker-compose.yaml up --build -d

#docker_compose_down:
#	@echo "Stopping docker-compose"
#	docker-compose -f docker/docker-compose.yaml down

#docker: go_build_docker docker_s3d

#docker_clean:
#	@echo "Removing Docker images and volumes"
#	docker rmi mulgadc/predastore:latest
#docker volume ls -f dangling=true
#yes | docker volume prune

#docker_test: docker docker_compose_up test docker_compose_down docker_clean

run:
	$(MAKE) go_build
	$(MAKE) go_run

clean:
	rm ./bin/sfs

.PHONY: go_build go_run build run test