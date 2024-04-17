###############################################################################
### build stage 
###############################################################################
FROM golang:1.22.2-alpine3.19 AS builder

## INSTALL DEPENDENCIES
RUN apk add --update --no-cache curl git make musl-dev gcc bash

WORKDIR /bus

COPY . .

# CONFIGURE GIT to use access token for private repos
RUN --mount=type=secret,id=_env,dst=.env \
    source .env && \
    git config --global url."https://${ELLA_ACCESS_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/"

ENV GOEXPERIMENT=rangefunc
ENV GOPRIVATE=ella.to/*

## TEST GO UNIT TESTS
RUN go test -race -timeout 50s ./... -v

## BUILD API 
RUN go build -ldflags="-w -s -X main.GitCommit=${GIT_COMMIT} -X main.Version=${VERSION}" -o ./bus-server cmd/server/main.go

###############################################################################
### run stage
###############################################################################
FROM alpine:3.19.1
COPY --from=builder /bus/bus-server ./server

EXPOSE 2021
CMD ["./server"]