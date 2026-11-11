FROM golang:1.23-alpine AS build

WORKDIR /app

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 go build -o /soup-job-queue .

FROM alpine:3.21

RUN adduser -D -H appuser

COPY --from=build /soup-job-queue /soup-job-queue

USER appuser

EXPOSE 8080

ENTRYPOINT ["/soup-job-queue"]
