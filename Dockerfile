FROM golang:alpine

WORKDIR /go/src/dynamodbdump
COPY glide.yaml *.go /go/src/dynamodbdump/

RUN apk update \
    && apk add --no-cache git \
    && go get github.com/Masterminds/glide \
    && go install github.com/Masterminds/glide \
    && glide install --strip-vendor
RUN go-wrapper install

CMD ["go-wrapper", "run"]
