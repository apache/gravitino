/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

export const providers = [
  {
    label: 'hadoop',
    value: 'hadoop',
    defaultProps: []
  },
  {
    label: 'hive',
    value: 'hive',
    defaultProps: [
      {
        key: 'metastore.uris',
        value: '',
        required: true,
        description: 'The Hive metastore URIs'
      }
    ]
  },
  {
    label: 'iceberg',
    value: 'lakehouse-iceberg',
    defaultProps: [
      {
        key: 'catalog-backend',
        value: 'hive',
        required: true,
        description: 'Iceberg catalog type choose properties',
        select: ['hive', 'jdbc']
      },
      {
        key: 'uri',
        value: '',
        required: true,
        description: 'Iceberg catalog uri config'
      },
      {
        key: 'warehouse',
        value: '',
        required: true,
        description: 'Iceberg catalog warehouse config'
      },
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        hide: 'hive',
        description: `"com.mysql.jdbc.Driver" or "com.mysql.cj.jdbc.Driver" for MySQL, "org.postgresql.Driver" for PostgreSQL`
      },
      {
        key: 'jdbc-user',
        value: '',
        required: true,
        hide: 'hive'
      },
      {
        key: 'jdbc-password',
        value: '',
        required: true,
        hide: 'hive'
      }
    ]
  },
  {
    label: 'mysql',
    value: 'jdbc-mysql',
    defaultProps: [
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver'
      },
      {
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:mysql://localhost:3306'
      },
      {
        key: 'jdbc-user',
        value: '',
        required: true
      },
      {
        key: 'jdbc-password',
        value: '',
        required: true
      }
    ]
  },
  {
    label: 'postgresql',
    value: 'jdbc-postgresql',
    defaultProps: [
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. org.postgresql.Driver'
      },
      {
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:postgresql://localhost:5432/your_database'
      },
      {
        key: 'jdbc-user',
        value: '',
        required: true
      },
      {
        key: 'jdbc-password',
        value: '',
        required: true
      },
      {
        key: 'jdbc-database',
        value: '',
        required: true
      }
    ]
  }
]
