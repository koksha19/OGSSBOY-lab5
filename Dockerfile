FROM golang:1.24 as build
WORKDIR /go/src/practice-5
COPY . .
RUN go test ./...
ENV CGO_ENABLED=0
RUN go install ./cmd/...
FROM alpine:latest
WORKDIR /opt/practice-5
COPY entry.sh /opt/practice-5/
RUN chmod +x /opt/practice-5/entry.sh
COPY --from=build /go/bin/* /opt/practice-5/
RUN ls /opt/practice-5
ENTRYPOINT ["/opt/practice-5/entry.sh"]
CMD ["server"]
