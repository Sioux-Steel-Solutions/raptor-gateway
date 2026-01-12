FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o raptor-gateway .

FROM scratch
COPY --from=builder /app/raptor-gateway /raptor-gateway
ENTRYPOINT ["/raptor-gateway"]
