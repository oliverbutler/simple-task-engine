FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o task-engine .

# Use a smaller image for the final build
FROM alpine:3.19

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/task-engine .

# Expose metrics port
EXPOSE 9090

# Run the application
CMD ["./task-engine"]
