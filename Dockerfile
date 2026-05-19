FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod ./
# go mod tidy generates go.sum and downloads deps in one step.
ENV GONOSUMDB="*"
ENV GOFLAGS="-mod=mod"
RUN go mod tidy

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /lb ./cmd/balancer

FROM alpine:3.19
RUN apk --no-cache add ca-certificates curl
WORKDIR /app
COPY --from=builder /lb /app/lb
EXPOSE 8080
ENTRYPOINT ["/app/lb"]
