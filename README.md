This is just a simplified example to use TransactionSynchronizationRegistry
to register transaction synchronization to store it into a separate
table when the transaction commits. Still not as good as
outbound table but doesn't need a secondary table. Could probably be
even further improved, this is just a simple example.

Create table:

```
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD="postgres" postgres

create table if not exists queue (id serial, kafka text not null);
```
