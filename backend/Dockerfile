FROM golang:1.23-alpine AS builder

WORKDIR /app

# Backend uses only stdlib — initialise a minimal module inline.
COPY backend/main.go .
RUN go mod init backend && \
    CGO_ENABLED=0 GOOS=linux go build -o /backend .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /backend /app/backend
EXPOSE 8080
ENTRYPOINT ["/app/backend"]
