/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

export const providers = [
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
        value: '',
        required: true,
        description: 'Iceberg catalog type choose properties'
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
      }
    ]
  },
  {
    label: 'mysql',
    value: 'jdbc-mysql',
    defaultProps: [
      {
        key: 'jdbc-url',
        value: '',
        required: true
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
        key: 'jdbc-url',
        value: '',
        required: true
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
