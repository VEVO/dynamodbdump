FROM golang:alpine

WORKDIR /go/src/dynamodbdump
COPY *.go /go/src/dynamodbdump/

RUN apk update \
    && apk add --no-cache git
RUN go-wrapper install

CMD ["go-wrapper", "run"]
