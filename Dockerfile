FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /server ./cmd/server
FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 simulator
USER simulator
WORKDIR /app
COPY --from=build /server /app/server
ENV HTTP_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/app/server"]
