---
title: 'Column commands'
slug: /cli-column
license: 'This software is licensed under the Apache License version 2.'
---

## Column commands 

Note that some commands are not supported due to the database used.
When setting the data type for a column, the following basic types are currently supported:

```
null, boolean, byte, ubyte, short, ushort, integer, uinteger, long,
ulong, float, double, date, time, timestamp, tztimestamp, intervalyear,
intervalday, uuid, string, binary
```

In addition decimal(precision,scale), fixed(length), fixedchar(length) and varchar(length).

### Show all columns

```bash
gcli.sh column list --name catalog_postgres.hr.departments
```

### Show column's audit information

```bash
gcli.sh column details --name catalog_postgres.hr.departments.name --audit
```

### Show a column's audit information

```bash
gcli.sh column details --name catalog_postgres.hr.departments.name --audit
```

### Add a column

```bash
gcli.sh column create --name catalog_postgres.hr.departments.value --datatype long
gcli.sh column create --name catalog_postgres.hr.departments.money --datatype "decimal(10,2)"
gcli.sh column create --name catalog_postgres.hr.departments.name --datatype "varchar(100)"
gcli.sh column create --name catalog_postgres.hr.departments.fullname \
  --datatype "varchar(250)" --default "Fred Smith" --null=false
```

### Delete a column

```bash
gcli.sh column delete --name catalog_postgres.hr.departments.money
```

### Update a column

```bash
gcli.sh column update --name catalog_postgres.hr.departments.value --rename values
gcli.sh column update --name catalog_postgres.hr.departments.values --datatype "varchar(500)"
gcli.sh column update --name catalog_postgres.hr.departments.values --position name
gcli.sh column update --name catalog_postgres.hr.departments.name --null true
```

