###############################################################################
### build stage 
###############################################################################
FROM golang:1.23-alpine3.20 AS builder

## INSTALL DEPENDENCIES
RUN apk add --update --no-cache curl git make musl-dev gcc bash

WORKDIR /src

COPY . .

## TEST GO UNIT TESTS
RUN go test -race -timeout 100s ./... -v

ARG GIT_COMMIT
ARG VERSION

## BUILD API 
RUN go build -ldflags="-w -s -X main.GitCommit=${GIT_COMMIT} -X main.Version=${VERSION}" -o ./bin/bus cmd/bus/main.go

###############################################################################
### run stage
###############################################################################
FROM alpine:3.20
COPY --from=builder /src/bin/bus ./bus

EXPOSE 2021
CMD ["./bus", "server"]