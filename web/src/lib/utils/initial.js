/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        description: 'The storage location of the fileset',
      },
    ],
  },
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
        description: 'The Apache Kafka brokers to connect to, allowing for multiple brokers separated by commas',
      },
    ],
  },
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
        description: 'The Apache Hive metastore URIs',
      },
    ],
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
        select: ['hive', 'jdbc'],
      },
      {
        key: 'uri',
        value: '',
        required: true,
        description: 'Apache Iceberg catalog uri config',
      },
      {
        key: 'warehouse',
        value: '',
        required: true,
        description: 'Apache Iceberg catalog warehouse config',
      },
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        hide: 'hive',
        description: `"com.mysql.jdbc.Driver" or "com.mysql.cj.jdbc.Driver" for MySQL, "org.postgresql.Driver" for PostgreSQL`,
      },
      {
        key: 'jdbc-user',
        value: '',
        required: true,
        hide: 'hive',
      },
      {
        key: 'jdbc-password',
        value: '',
        required: true,
        hide: 'hive',
      },
    ],
  },
  {
    label: 'MySQL',
    value: 'jdbc-mysql',
    defaultProps: [
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver',
      },
      {
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:mysql://localhost:3306',
      },
      {
        key: 'jdbc-user',
        value: '',
        required: true,
      },
      {
        key: 'jdbc-password',
        value: '',
        required: true,
      },
    ],
  },
  {
    label: 'PostgreSQL',
    value: 'jdbc-postgresql',
    defaultProps: [
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. org.postgresql.Driver',
      },
      {
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:postgresql://localhost:5432/your_database',
      },
      {
        key: 'jdbc-user',
        value: '',
        required: true,
      },
      {
        key: 'jdbc-password',
        value: '',
        required: true,
      },
      {
        key: 'jdbc-database',
        value: '',
        required: true,
      },
    ],
  },
  {
    label: 'Apache Doris',
    value: 'jdbc-doris',
    defaultProps: [
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. com.mysql.jdbc.Driver',
      },
      {
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:mysql://localhost:9030',
      },
      {
        key: 'jdbc-user',
        value: '',
        required: true,
      },
      {
        key: 'jdbc-password',
        value: '',
        required: true,
      },
    ],
  },
]
