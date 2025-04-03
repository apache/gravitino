---
title: 'Table commands'
slug: /cli-table
license: 'This software is licensed under the Apache License version 2.'
---

## Table commands 

### List all tables

```bash
gcli.sh table list --name catalog_postgres.hr
```

### Show a table'sdetails

```bash
gcli.sh table details --name catalog_postgres.hr.departments
```

#### Show the audit information for a table

```bash
gcli.sh table details --name catalog_postgres.hr.departments --audit
```

#### Show the distribution information for a table

```bash
gcli.sh table details --name catalog_postgres.hr.departments --distribution
```

### Show a table's partition information

```bash
gcli.sh table details --name catalog_postgres.hr.departments --partition
```

### Show a table's sort order information

```bash
gcli.sh table details --name catalog_postgres.hr.departments --sortorder
```

### Show the index information about a table

```bash
gcli.sh table details --name catalog_mysql.db.iceberg_namespace_properties --index
```

### Create a table

```bash
gcli.sh table create --name catalog_postgres.hr.salaries --comment "comment" --columnfile ~/table.csv
```

When creating a table, you will need a CSV file that contains the column information including:

- the name of the column
- the column data type
- a  comment to the column, default to null
- whether the column is nullable, default to true
- whether the column is auto incremented, default to false
- the default value, set based on data type
- the default type for the column

The following is a sample CSV file:

```text
Name,Datatype,Comment,Nullable,AutoIncrement,DefaultValue,DefaultType
name,String,person's name
ID,Integer,unique id,false,true
location,String,city they work in,false,false,Sydney,String
```

### Delete a table

```bash
gcli.sh table delete --name catalog_postgres.hr.salaries
```

### Display a tables's properties

```bash
gcli.sh table properties --name catalog_postgres.hr.salaries
```

### Set a tables's property

```bash
gcli.sh table set --name catalog_postgres.hr.salaries --property test --value value
```

### Remove a tables's property

```bash
gcli.sh table remove --name catalog_postgres.hr.salaries --property test
```

