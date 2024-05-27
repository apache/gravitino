/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

export const filesetProviders = [
  {
    label: 'Apache Hadoop',
    value: 'hadoop',
    defaultProps: [
      {
        key: 'location',
        value: '',
        required: false,
        description: 'The storage location of the fileset'
      }
    ]
  }
]

export const messagingProviders = [
  {
    label: 'Apache Kafka',
    value: 'kafka',
    defaultProps: [
      {
        key: 'bootstrap.servers',
        value: '',
        required: true,
        description: 'The Apache Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them'
      }
    ]
  }
]

export const providers = [
  {
    label: 'Apache Hive',
    value: 'hive',
    defaultProps: [
      {
        key: 'metastore.uris',
        value: '',
        required: true,
        description: 'The Apache Hive metastore URIs'
      }
    ]
  },
  {
    label: 'Apache Iceberg',
    value: 'lakehouse-iceberg',
    defaultProps: [
      {
        key: 'catalog-backend',
        value: 'hive',
        required: true,
        description: 'Apache Iceberg catalog type choose properties',
        select: ['hive', 'jdbc']
      },
      {
        key: 'uri',
        value: '',
        required: true,
        description: 'Apache Iceberg catalog uri config'
      },
      {
        key: 'warehouse',
        value: '',
        required: true,
        description: 'Apache Iceberg catalog warehouse config'
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
    label: 'MySQL',
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
    label: 'PostgreSQL',
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
  },
  {
    label: 'Apache Doris',
    value: 'jdbc-doris',
    defaultProps: [
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. com.mysql.jdbc.Driver'
      },
      {
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:mysql://localhost:9030'
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
  }
]
