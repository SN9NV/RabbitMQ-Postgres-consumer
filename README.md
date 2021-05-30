POC for a parallel RabbitMQ consumer that writes the messages into a Postgres database. I had an issue where my RabbitMQ and Postgres had quite a high ping between them (~200ms), so parallelism of the consumer was required.

Creating a consumer per Postgres pool connection solved this problem and was very efficient with both CPU and memory.

Create a table in the database
```sql
CREATE TABLE items (
    id BIGINT PRIMARY KEY,
    value BIGINT
);
```

Postgres and RabbitMQ connections are configured through environment variables

|     ENV name      |                  Usage                  |  Example  |
|-------------------|-----------------------------------------|-----------|
| PG_HOST           | Postgres hostname/IP                    | localhost |
| PG_PORT           | Postgres port number                    | 5432      |
| PG_USER           | Postgres user                           | postgres  |
| PG_PASSWORD       | Postgres password                       | password  |
| PG_DATABASE       | Postgres database                       | postgres  |
| PG_POOL_MAX_CONNS | Postgres pool max number of connections | 10        |
| MQ_HOST           | RabbitMQ hostname/IP                    | localhost |
| MQ_PORT           | RabbitMQ port                           | 5672      |
| MQ_QUEUE          | RabbitMQ queue name                     | items     |
