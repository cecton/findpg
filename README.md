findpg
======

Find the best suited version of PostgreSQL to restore a dump


Examples
========

```
./findpg.py --clean --dbname=test --dump=/path/to/unknown_dump.sql \
    postgres://%2Frun%2Fpostgresql-8.4:5432/usr/lib64/postgresql-8.4/bin \
    postgres://%2Frun%2Fpostgresql-9.1:5432/usr/lib64/postgresql-9.1/bin \
    postgres://%2Frun%2Fpostgresql-9.2:5432/usr/lib64/postgresql-9.2/bin \
    postgres://%2Frun%2Fpostgresql-9.3:5432/usr/lib64/postgresql-9.3/bin
```

```python
from urlparse import urlparse

with open("dump_file.sql", 'rb') as dumpobj:
    postgres = restore(dumpobj, 'dbname',
       [urlparse("postgres://localhost:5432/usr/lib64/postgresql-8.4/bin"),
        urlparse("postgres://localhost:5433/usr/lib64/postgresql-9.1/bin")],
       drop=True)
```
