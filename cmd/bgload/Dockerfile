FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
ENV GONOSUMDB="*"
ENV GOFLAGS="-mod=mod"
RUN go mod tidy
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bgload ./cmd/bgload

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /bgload /app/bgload
ENTRYPOINT ["/app/bgload"]
