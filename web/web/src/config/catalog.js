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

export const checkCatalogIcon = ({ type, provider }) => {
  switch (type) {
    case 'relational':
      switch (provider) {
        case 'hive':
          return 'custom-icons-hive'
        case 'lakehouse-iceberg':
          return 'openmoji:iceberg'
        case 'jdbc-mysql':
          return 'devicon:mysql-wordmark'
        case 'jdbc-postgresql':
          return 'devicon:postgresql-wordmark'
        case 'jdbc-doris':
          return 'custom-icons-doris'
        case 'lakehouse-paimon':
          return 'custom-icons-paimon'
        case 'lakehouse-hudi':
          return 'custom-icons-hudi'
        case 'jdbc-oceanbase':
          return 'custom-icons-oceanbase'
        case 'jdbc-starrocks':
          return 'custom-icons-starrocks'
        case 'lakehouse-generic':
          return 'custom-icons-lakehouse'
        default:
          return 'bx:book'
      }
    case 'messaging':
      return 'skill-icons:kafka'
    case 'fileset':
      return 'twemoji:file-folder'
    case 'model':
      return 'carbon:machine-learning-model'
    default:
      return 'bx:book'
  }
}

export const tableDefaultProps = {
  hive: [
    {
      key: 'table-type',
      defaultValue: 'MANAGED_TABLE',
      select: ['MANAGED_TABLE', 'EXTERNAL_TABLE']
    },
    {
      key: 'location',
      defaultValue: '',
      description: 'Hadoop catalog storage location'
    },
    {
      key: 'format',
      defaultValue: 'TEXTFILE',
      select: ['TEXTFILE', 'SEQUENCEFILE', 'RCFILE', 'ORC', 'PARQUET', 'AVRO', 'JSON', 'CSV', 'REGEX']
    },
    {
      key: 'input-format',
      defaultValue: 'org.apache.hadoop.mapred.TextInputFormat',
      defaultValueOptions: {
        TEXTFILE: 'org.apache.hadoop.mapred.TextInputFormat',
        SEQUENCEFILE: 'org.apache.hadoop.mapred.SequenceFileInputFormat',
        RCFILE: 'org.apache.hadoop.hive.ql.io.RCFileInputFormat',
        ORC: 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',
        PARQUET: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        AVRO: 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat',
        JSON: 'org.apache.hadoop.mapred.TextInputFormat',
        CSV: 'org.apache.hadoop.mapred.TextInputFormat',
        REGEX: 'org.apache.hadoop.mapred.TextInputFormat'
      }
    },
    {
      key: 'output-format',
      defaultValue: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
      defaultValueOptions: {
        TEXTFILE: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        SEQUENCEFILE: 'org.apache.hadoop.mapred.SequenceFileOutputFormat',
        RCFILE: 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat',
        ORC: 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',
        PARQUET: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        AVRO: 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat',
        JSON: 'org.apache.hadoop.mapred.TextOutputFormat',
        CSV: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        REGEX: 'org.apache.hadoop.mapred.TextOutputFormat'
      }
    },
    {
      key: 'serde-lib',
      defaultValue: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
      defaultValueOptions: {
        TEXTFILE: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
        SEQUENCEFILE: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
        RCFILE: 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe',
        ORC: 'org.apache.hadoop.hive.ql.io.orc.OrcSerde',
        PARQUET: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
        AVRO: 'org.apache.hadoop.hive.serde2.avro.AvroSerDe',
        JSON: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
        CSV: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
        REGEX: 'org.apache.hadoop.hive.serde2.RegexSerDe'
      }
    }
  ],
  'jdbc-mysql': [
    {
      key: 'engine',
      defaultValue: 'InnoDB',
      select: ['InnoDB', 'MyISAM', 'MEMORY', 'CSV', 'ARCHIVE', 'BLACKHOLE', 'MRG_MYISAM']
    },
    {
      key: 'auto-increment-offset',
      defaultValue: '',
      description: 'autoIncrementOffsetDesc'
    }
  ],
  'lakehouse-generic': [
    {
      key: 'location',
      defaultValue: '',
      description: 'The storage location of the table. Required if not set in catalog and schema.'
    },
    {
      key: 'format',
      defaultValue: 'lance',
      disabled: true,
      description: 'The format of the table'
    }
  ]
}

export const filesetLocationProviders = {
  'builtin-local': 'file:/',
  'builtin-hdfs': 'hdfs://',
  s3: 's3a://',
  gcs: 'gs://',
  oss: 'oss://',
  abs: 'abfss://'
}

export const entityDefaultProps = {
  hadoop: [
    {
      key: 'filesystem-providers',
      defaultValue: ['builtin-local', 'builtin-hdfs'],
      select: Object.keys(filesetLocationProviders),
      multiple: true,
      description: 'The file system providers to add'
    },
    {
      key: 'location',
      defaultValue: '',
      selectBefore: Object.values(filesetLocationProviders),
      description: 'Hadoop catalog storage location'
    }
  ]
}

export const locationDefaultProps = {
  s3: [
    {
      key: 's3-endpoint',
      defaultValue: '',
      description: 'fileset.s3EndpointDesc'
    },
    {
      key: 's3-access-key-id',
      defaultValue: '',
      description: 'fileset.s3AccessKeyDesc'
    },
    {
      key: 's3-secret-access-key',
      defaultValue: '',
      description: 'fileset.s3SecretKeyDesc'
    }
  ],
  gcs: [
    {
      key: 'gcs-service-account-file',
      defaultValue: '',
      description: 'fileset.gcsServiceAccountFileDesc'
    }
  ],
  oss: [
    {
      key: 'oss-endpoint',
      defaultValue: '',
      description: 'fileset.ossEndpointDesc'
    },
    {
      key: 'oss-access-key-id',
      defaultValue: '',
      description: 'fileset.ossAccessKeyDesc'
    },
    {
      key: 'oss-secret-access-key',
      defaultValue: '',
      description: 'fileset.ossSecretKeyDesc'
    }
  ],
  abs: [
    {
      key: 'azure-storage-account-name',
      defaultValue: '',
      description: 'fileset.abfsAccountNameDesc'
    },
    {
      key: 'azure-storage-account-key',
      defaultValue: '',
      description: 'fileset.abfsAccountKeyDesc'
    }
  ]
}

export const rangerDefaultProps = [
  {
    label: 'Authorization Provider',
    key: 'authorization-provider',
    value: 'ranger',
    disabled: true
  },
  {
    label: 'Ranger Auth Type',
    key: 'authorization.ranger.auth.type',
    value: 'simple',
    disabled: true
  },
  {
    label: 'Ranger Admin URL',
    key: 'authorization.ranger.admin.url',
    value: ''
  },
  {
    label: 'Ranger Username',
    key: 'authorization.ranger.username',
    value: ''
  },
  {
    label: 'Ranger Password',
    key: 'authorization.ranger.password',
    value: ''
  },
  {
    label: 'Ranger Service Type',
    key: 'authorization.ranger.service.type',
    value: '',
    select: ['HadoopSQL', 'HDFS'],
    description: 'rangerServiceTypeDesc'
  },
  {
    label: 'Ranger Service Name',
    key: 'authorization.ranger.service.name',
    value: '',
    description: 'rangerServiceNameDesc'
  },
  {
    label: 'Ranger service initialize',
    key: 'authorization.ranger.service.create-if-absent',
    value: '',
    description: 'serviceInitializeDesc'
  }
]

export const providerBase = {
  hive: {
    label: 'Apache Hive',
    defaultProps: [
      {
        label: 'Metastore URIs',
        key: 'metastore.uris',
        value: '',
        required: true,
        description: 'The Apache Hive metastore URIs'
      }
    ]
  },
  'lakehouse-iceberg': {
    label: 'Apache Iceberg',
    defaultProps: [
      {
        label: 'Catalog Backend',
        key: 'catalog-backend',
        value: 'hive',
        required: true,
        description: 'Apache Iceberg catalog type choose properties',
        select: ['hive', 'jdbc']
      },
      {
        label: 'URI',
        key: 'uri',
        value: '',
        required: true,
        description: 'Apache Iceberg catalog uri config'
      },
      {
        label: 'Warehouse',
        key: 'warehouse',
        value: '',
        required: true,
        description: 'Apache Iceberg catalog warehouse config'
      },
      {
        label: 'JDBC Driver',
        key: 'jdbc-driver',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive'],
        description:
          '"com.mysql.jdbc.Driver" or "com.mysql.cj.jdbc.Driver" for MySQL, "org.postgresql.Driver" for PostgreSQL'
      },
      {
        label: 'JDBC User',
        key: 'jdbc-user',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive']
      },
      {
        label: 'JDBC Password',
        key: 'jdbc-password',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive']
      },
      {
        label: 'Authentication Type',
        key: 'authentication.type',
        value: 'simple',
        required: false,
        description:
          'The type of authentication for Paimon catalog backend, currently Gravitino only supports Kerberos and simple',
        select: ['simple', 'Kerberos']
      },
      {
        label: 'Authentication Kerberos Principal',
        key: 'authentication.kerberos.principal',
        value: '',
        required: true,
        description: 'The principal of the Kerberos authentication.',
        parentField: 'authentication.type',
        hide: ['simple']
      },
      {
        label: 'Authentication Kerberos Keytab Uri',
        key: 'authentication.kerberos.keytab-uri',
        value: '',
        required: true,
        description: 'The URI of The keytab for the Kerberos authentication.',
        parentField: 'authentication.type',
        hide: ['simple']
      }
    ]
  },
  'jdbc-mysql': {
    label: 'MySQL',
    defaultProps: [
      {
        label: 'JDBC Driver',
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver'
      },
      {
        label: 'JDBC URL',
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:mysql://localhost:3306'
      },
      {
        label: 'JDBC User',
        key: 'jdbc-user',
        value: '',
        required: true
      },
      {
        label: 'JDBC Password',
        key: 'jdbc-password',
        value: '',
        required: true
      }
    ]
  },
  'jdbc-postgresql': {
    label: 'PostgreSQL',
    defaultProps: [
      {
        label: 'JDBC Driver',
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. org.postgresql.Driver'
      },
      {
        label: 'JDBC URL',
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:postgresql://localhost:5432/your_database'
      },
      {
        label: 'JDBC User',
        key: 'jdbc-user',
        value: '',
        required: true
      },
      {
        label: 'JDBC Password',
        key: 'jdbc-password',
        value: '',
        required: true
      },
      {
        label: 'JDBC Database',
        key: 'jdbc-database',
        value: '',
        required: true
      }
    ]
  },
  'jdbc-doris': {
    label: 'Apache Doris',
    defaultProps: [
      {
        label: 'JDBC Driver',
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver'
      },
      {
        label: 'JDBC URL',
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:mysql://localhost:9030'
      },
      {
        label: 'JDBC User',
        key: 'jdbc-user',
        value: '',
        required: true
      },
      {
        label: 'JDBC Password',
        key: 'jdbc-password',
        value: '',
        required: true
      }
    ]
  },
  'jdbc-starrocks': {
    label: 'StarRocks',
    defaultProps: [
      {
        label: 'JDBC Driver',
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver'
      },
      {
        label: 'JDBC URL',
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:mysql://localhost:9030'
      },
      {
        label: 'JDBC User',
        key: 'jdbc-user',
        value: '',
        required: true
      },
      {
        label: 'JDBC Password',
        key: 'jdbc-password',
        value: '',
        required: true
      }
    ]
  },
  'lakehouse-paimon': {
    label: 'Apache Paimon',
    defaultProps: [
      {
        label: 'Catalog Backend',
        key: 'catalog-backend',
        value: 'filesystem',
        required: true,
        select: ['filesystem', 'hive', 'jdbc'],
        description: 'paimonBackendDesc'
      },
      {
        label: 'URI',
        key: 'uri',
        value: '',
        required: true,
        description: 'Only supports "filesystem" now.',
        parentField: 'catalog-backend',
        hide: ['filesystem']
      },
      {
        label: 'Warehouse',
        key: 'warehouse',
        value: '',
        required: true,
        description: 'e.g. file:/user/hive/warehouse-paimon/ or hdfs://namespace/hdfs/path'
      },
      {
        label: 'JDBC Driver',
        key: 'jdbc-driver',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive', 'filesystem'],
        description:
          '"com.mysql.jdbc.Driver" or "com.mysql.cj.jdbc.Driver" for MySQL, "org.postgresql.Driver" for PostgreSQL'
      },
      {
        label: 'JDBC User',
        key: 'jdbc-user',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive', 'filesystem']
      },
      {
        label: 'JDBC Password',
        key: 'jdbc-password',
        value: '',
        required: true,
        parentField: 'catalog-backend',
        hide: ['hive', 'filesystem']
      },
      {
        label: 'Authentication Type',
        key: 'authentication.type',
        value: 'simple',
        required: false,
        description:
          'The type of authentication for Paimon catalog backend, currently Gravitino only supports Kerberos and simple',
        select: ['simple', 'Kerberos']
      },
      {
        label: 'Authentication Kerberos Principal',
        key: 'authentication.kerberos.principal',
        value: '',
        required: true,
        description: 'The principal of the Kerberos authentication.',
        parentField: 'authentication.type',
        hide: ['simple']
      },
      {
        label: 'Authentication Kerberos Keytab Uri',
        key: 'authentication.kerberos.keytab-uri',
        value: '',
        required: true,
        description: 'The URI of The keytab for the Kerberos authentication.',
        parentField: 'authentication.type',
        hide: ['simple']
      }
    ]
  },
  'lakehouse-generic': {
    label: 'Lakehouse Generic',
    defaultProps: []
  },
  'lakehouse-hudi': {
    label: 'Apache Hudi',
    defaultProps: [
      {
        label: 'Catalog Backend',
        key: 'catalog-backend',
        value: 'hms',
        defaultValue: 'hms',
        required: true,
        select: ['hms']
      },
      {
        label: 'URI',
        key: 'uri',
        value: '',
        required: true,
        description: 'Apache Hudi catalog uri config'
      }
    ]
  },
  'jdbc-oceanbase': {
    label: 'OceanBase',
    defaultProps: [
      {
        label: 'JDBC Driver',
        key: 'jdbc-driver',
        value: '',
        required: true,
        description: 'e.g. com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver or com.oceanbase.jdbc.Driver'
      },
      {
        label: 'JDBC URL',
        key: 'jdbc-url',
        value: '',
        required: true,
        description: 'e.g. jdbc:mysql://localhost:2881 or jdbc:oceanbase://localhost:2881'
      },
      {
        label: 'JDBC User',
        key: 'jdbc-user',
        value: '',
        required: true
      },
      {
        label: 'JDBC Password',
        key: 'jdbc-password',
        value: '',
        required: true
      }
    ]
  },
  hadoop: {
    label: 'Apache Hadoop',
    defaultProps: []
  },
  kafka: {
    label: 'Apache Kafka',
    defaultProps: [
      {
        label: 'Bootstrap Servers',
        key: 'bootstrap.servers',
        value: '',
        required: true,
        description: 'The Apache Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them'
      }
    ]
  },
  model: {
    label: 'Model',
    defaultProps: []
  }
}

export const Location = [
  {
    label: 'Apache Hadoop',
    value: 'hadoop',
    description: 'An open source distributed data storage system'
  }
]

export const messagingProviders = [
  {
    label: 'Apache Kafka',
    value: 'kafka',
    description: 'A distributed streaming platform'
  }
]

export const relationalProviders = [
  {
    label: 'Apache Doris',
    value: 'jdbc-doris',
    description: 'Open source, real-time data warehouse'
  },
  {
    label: 'Apache Hive',
    value: 'hive',
    description: 'A distributed, fault-tolerant data warehouse system'
  },
  {
    label: 'Apache Hudi',
    value: 'lakehouse-hudi',
    description: 'An open source data lake platform'
  },
  {
    label: 'Apache Iceberg',
    value: 'lakehouse-iceberg',
    description: 'The open table format for analytic datasets'
  },
  {
    label: 'Apache Paimon',
    value: 'lakehouse-paimon',
    description: 'A realtime lakehouse architecture with Flink and Spark for both streaming and batch operations'
  },
  {
    label: 'Lakehouse Generic',
    value: 'lakehouse-generic',
    description: 'A generic lakehouse catalog supporting multiple table formats'
  },
  {
    label: 'MySQL',
    value: 'jdbc-mysql',
    description: 'A fully managed database service'
  },
  {
    label: 'OceanBase',
    value: 'jdbc-oceanbase',
    description: 'The multi-cloud distributed database for mission-critical workloads at any scale'
  },
  {
    label: 'PostgreSQL',
    value: 'jdbc-postgresql',
    description: "The world's most advanced open source relational database"
  },
  {
    label: 'StarRocks',
    value: 'jdbc-starrocks',
    description: 'An open source, high performance analytical database'
  }
]
