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
        description: 'The Apache Kafka brokers to connect to, allowing for multiple brokers separated by commas'
      }
    ]
  }
]

export const providers = [
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
  },
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
    label: 'Apache Hudi',
    value: 'lakehouse-hudi',
    defaultProps: [
      {
        key: 'catalog-backend',
        value: 'hms',
        defaultValue: 'hms',
        required: true,
        select: ['hms'],
        description: 'Apache Hudi catalog type choose properties'
      },
      {
        key: 'uri',
        value: '',
        required: true,
        description: 'Apache Hudi catalog uri config'
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
        defaultValue: 'hive',
        required: true,
        description: 'Apache Iceberg catalog type choose properties',
        select: ['hive', 'jdbc', 'rest']
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
        description: 'Apache Iceberg catalog warehouse config'
      },
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive', 'rest'],
        description: `"com.mysql.jdbc.Driver" or "com.mysql.cj.jdbc.Driver" for MySQL, "org.postgresql.Driver" for PostgreSQL`
      },
      {
        key: 'jdbc-user',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive', 'rest']
      },
      {
        key: 'jdbc-password',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive', 'rest']
      },
      {
        key: 'authentication.type',
        value: 'simple',
        defaultValue: 'simple',
        required: false,
        description:
          'The type of authentication for Paimon catalog backend, currently Gravitino only supports Kerberos and simple',
        select: ['simple', 'Kerberos']
      },
      {
        key: 'authentication.kerberos.principal',
        value: '',
        required: true,
        description: 'The principal of the Kerberos authentication.',
        parentField: 'authentication.type',
        hide: ['simple']
      },
      {
        key: 'authentication.kerberos.keytab-uri',
        value: '',
        required: true,
        description: 'The URI of The keytab for the Kerberos authentication.',
        parentField: 'authentication.type',
        hide: ['simple']
      }
    ]
  },
  {
    label: 'Apache Paimon',
    value: 'lakehouse-paimon',
    defaultProps: [
      {
        key: 'catalog-backend',
        value: 'filesystem',
        defaultValue: 'filesystem',
        required: true,
        select: ['filesystem', 'hive', 'jdbc']
      },
      {
        key: 'uri',
        value: '',
        required: true,
        description:
          'e.g. thrift://127.0.0.1:9083 or jdbc:postgresql://127.0.0.1:5432/db_name or jdbc:mysql://127.0.0.1:3306/metastore_db',
        parentField: 'catalog-backend',
        hide: ['filesystem']
      },
      {
        key: 'warehouse',
        value: '',
        required: true,
        description: 'e.g. file:///user/hive/warehouse-paimon/ or hdfs://namespace/hdfs/path'
      },
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive', 'filesystem'],
        description: `"com.mysql.jdbc.Driver" or "com.mysql.cj.jdbc.Driver" for MySQL, "org.postgresql.Driver" for PostgreSQL`
      },
      {
        key: 'jdbc-user',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive', 'filesystem']
      },
      {
        key: 'jdbc-password',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive', 'filesystem']
      },
      {
        key: 'authentication.type',
        value: 'simple',
        defaultValue: 'simple',
        required: false,
        description:
          'The type of authentication for Paimon catalog backend, currently Gravitino only supports Kerberos and simple',
        select: ['simple', 'Kerberos']
      },
      {
        key: 'authentication.kerberos.principal',
        value: '',
        required: true,
        description: 'The principal of the Kerberos authentication.',
        parentField: 'authentication.type',
        hide: ['simple']
      },
      {
        key: 'authentication.kerberos.keytab-uri',
        value: '',
        required: true,
        description: 'The URI of The keytab for the Kerberos authentication.',
        parentField: 'authentication.type',
        hide: ['simple']
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
    label: 'OceanBase',
    value: 'jdbc-oceanbase',
    defaultProps: [
      {
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver or com.oceanbase.jdbc.Driver'
      },
      {
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:mysql://localhost:2881 or jdbc:oceanbase://localhost:2881'
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
    label: 'StarRocks',
    value: 'jdbc-starrocks',
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

const parameterizedColumnTypes = {
  char: {
    params: ['length'],
    validateParams: params => {
      if (params.length !== 1) {
        return {
          valid: false,
          message: 'Please set length'
        }
      }

      const length = params[0]

      if (length <= 0) {
        return {
          valid: false,
          message: 'The length must be greater than 0'
        }
      }

      return {
        valid: true
      }
    }
  },
  decimal: {
    params: ['precision', 'scale'],
    validateParams: params => {
      if (params.length !== 2) {
        return {
          valid: false,
          message: 'Please set precision and scale'
        }
      }

      const [param1, param2] = params
      if (param1 <= 0 || param1 > 38) {
        return {
          valid: false,
          message: 'The precision must be between 1 and 38'
        }
      }

      if (param2 < 0 || param2 > param1) {
        return {
          valid: false,
          message: 'The scale must be between 0 and the precision'
        }
      }

      return {
        valid: true
      }
    }
  },
  fixed: {
    params: ['length'],
    validateParams: params => {
      if (params.length !== 1) {
        return {
          valid: false,
          message: 'Please set length'
        }
      }

      const length = params[0]

      if (length <= 0) {
        return {
          valid: false,
          message: 'The length must be greater than 0'
        }
      }

      return {
        valid: true
      }
    }
  },
  varchar: {
    params: ['length'],
    validateParams: params => {
      if (params.length !== 1) {
        return {
          valid: false,
          message: 'Please set length'
        }
      }

      const length = params[0]

      if (length <= 0) {
        return {
          valid: false,
          message: 'The length must be greater than 0'
        }
      }

      return {
        valid: true
      }
    }
  }
}

export const getParameterizedColumnType = type => {
  if (Object.hasOwn(parameterizedColumnTypes, type)) {
    return parameterizedColumnTypes[type]
  }
}

const relationalColumnTypeMap = {
  'lakehouse-iceberg': [
    'binary',
    'boolean',
    'date',
    'decimal',
    'double',
    'fixed',
    'float',
    'integer',
    'long',
    'string',
    'time',
    'timestamp',
    'timestamp_tz',
    'uuid'
  ],
  hive: [
    'binary',
    'boolean',
    'byte',
    'char',
    'date',
    'decimal',
    'double',
    'float',
    'integer',
    'interval_day',
    'interval_year',
    'long',
    'short',
    'string',
    'timestamp',
    'varchar'
  ],
  'jdbc-mysql': [
    'binary',
    'byte',
    'byte unsigned',
    'char',
    'date',
    'decimal',
    'double',
    'float',
    'integer',
    'integer unsigned',
    'long',
    'long unsigned',
    'short',
    'short unsigned',
    'string',
    'time',
    'timestamp',
    'varchar'
  ],
  'jdbc-postgresql': [
    'binary',
    'boolean',
    'char',
    'date',
    'decimal',
    'double',
    'float',
    'integer',
    'long',
    'short',
    'string',
    'time',
    'timestamp',
    'timestamp_tz',
    'varchar'
  ],
  'jdbc-starrocks': [
    'binary',
    'boolean',
    'byte',
    'char',
    'date',
    'decimal',
    'double',
    'float',
    'integer',
    'long',
    'short',
    'string',
    'timestamp',
    'varchar'
  ],
  'jdbc-doris': [
    'boolean',
    'byte',
    'char',
    'date',
    'decimal',
    'double',
    'float',
    'integer',
    'long',
    'short',
    'string',
    'timestamp',
    'varchar'
  ],
  'lakehouse-paimon': [
    'binary',
    'boolean',
    'byte',
    'char',
    'date',
    'decimal',
    'double',
    'fixed',
    'float',
    'integer',
    'long',
    'short',
    'string',
    'time',
    'timestamp',
    'timestamp_tz',
    'varchar'
  ],
  'jdbc-oceanbase': [
    'binary',
    'byte',
    'byte unsigned',
    'char',
    'date',
    'decimal',
    'double',
    'float',
    'integer',
    'integer unsigned',
    'long',
    'long unsigned',
    'short',
    'short unsigned',
    'string',
    'time',
    'timestamp',
    'varchar'
  ]
}

export const getRelationalColumnType = catalog => {
  if (Object.hasOwn(relationalColumnTypeMap, catalog)) {
    return relationalColumnTypeMap[catalog]
  }

  return []
}

const relationalTablePropInfoMap = {
  hive: {
    reserved: ['comment', 'EXTERNAL', 'numFiles', 'totalSize', 'transient_lastDdlTime'],
    immutable: [
      'format',
      'input-format',
      'location',
      'output-format',
      'serde-name',
      'serde-lib',
      'serde.parameter',
      'table-type'
    ],
    allowDelete: true,
    allowAdd: true
  },
  'jdbc-starrocks': {
    reserved: [],
    allowDelete: true,
    allowAdd: true
  },
  'jdbc-doris': {
    reserved: [],
    allowDelete: true,
    allowAdd: true
  },
  'jdbc-mysql': {
    reserved: [],
    immutable: ['auto-increment-offset', 'engine'],
    allowDelete: false,
    allowAdd: true
  },
  'jdbc-oceanbase': {
    reserved: [],
    immutable: [],
    allowDelete: false,
    allowAdd: false
  },
  'jdbc-postgresql': {
    reserved: [],
    immutable: [],
    allowDelete: false,
    allowAdd: false
  },
  'lakehouse-hudi': {
    reserved: [],
    immutable: [],
    allowDelete: true,
    allowAdd: true
  },
  'lakehouse-iceberg': {
    reserved: [
      'cherry-pick-snapshot-id',
      'comment',
      'creator',
      'current-snapshot-id',
      'identifier-fields',
      'sort-order',
      'write.distribution-mode'
    ], // Can't be set or modified
    immutable: ['location', 'provider', 'format', 'format-version'], // Can't be modified after creation
    allowDelete: true,
    allowAdd: true
  },
  'lakehouse-paimon': {
    reserved: [
      'bucket-key',
      'comment',
      'merge-engine',
      'owner',
      'partition',
      'primary-key',
      'rowkind.field',
      'sequence.field'
    ],
    immutable: ['merge-engine', 'rowkind.field', 'sequence.field'],
    allowDelete: true,
    allowAdd: true
  }
}

export const getRelationalTablePropInfo = catalog => {
  if (Object.hasOwn(relationalTablePropInfoMap, catalog)) {
    return relationalTablePropInfoMap[catalog]
  }

  return {
    reserved: [],
    immutable: [],
    allowDelete: true,
    allowAdd: true
  }
}
