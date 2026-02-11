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

export const dialogContentMaxHeigth = 500

export const maxTreeWidth = 700

export const mismatchName =
  'Must start with a letter, digit, or underscore, can include alphanumeric characters, underscores, slashes (/), equal signs (=), or hyphens (-), and must be between 1 and 64 characters long!'

export const mismatchUsername =
  'Must start with a letter/underscore (_), contain only alphanumeric characters (Aa-Zz,0-9), underscore (_), center (-), dot (.) or the ‘@’ character.'

export const mismatchForKey =
  'Must start with a letter/underscore(_), contain only alphanumeric characters (Aa-Zz,0-9) or underscores (_), hyphens(-), or dots(.)!'

export const validateMessages = {
  required: 'Please enter the ${label}!',
  string: {
    max: 'The ${label} must be less than ${max} characters!'
  },
  pattern: {
    mismatch: mismatchName,
    mismatchusername: mismatchUsername,
    mismatchforkey: mismatchForKey
  }
}

export const supportedObjectTypesMap = {
  CATALOG: 'catalog',
  SCHEMA: 'schema',
  TABLE: 'table',
  FILESET: 'fileset',
  TOPIC: 'topic',
  MODEL: 'model'
}

export const ColumnTypeColorEnum = {
  boolean: 'blue',
  short: 'blue',
  integer: 'blue',
  long: 'blue',
  float: 'blue',
  double: 'blue',
  decimal: 'blue',
  fixed: 'blue',

  date: '',
  time: '',
  timestamp: '',
  timestamp_tz: '',
  interval_day: '',
  interval_year: '',

  string: 'orange',
  char: 'orange',
  varchar: 'orange',

  byte: 'green',
  uuid: 'green',
  binary: 'green',

  objectType: 'magenta'
}

export const jobStatusEnum = {
  queued: 'queued',
  started: 'started',
  failed: 'failed',
  succeeded: 'succeeded',
  cancelling: 'cancelling',
  cancelled: 'cancelled'
}

export const ColumnType = [
  'boolean',
  'byte',
  'short',
  'integer',
  'long',
  'float',
  'double',
  'date',
  'time',
  'timestamp',
  'timestamp_tz',
  'string',
  'interval_day',
  'interval_year',
  'uuid',
  'binary',
  'decimal'
]

export const ColumnTypeForUnsigned = ['byte unsigned', 'short unsigned', 'integer unsigned', 'long unsigned']

export const ColumnTypeSupportAutoIncrement = [
  'byte',
  'short',
  'integer',
  'long',
  'byte unsigned',
  'short unsigned',
  'integer unsigned',
  'long unsigned'
]

export const ColumnWithParamType = ['char', 'varchar', 'fixed']

export const ColumnSpesicalType = ['union', 'list', 'map', 'struct']

export const UnsupportColumnType = {
  'lakehouse-iceberg': ['varchar', 'char', 'byte', 'short', 'interval_day', 'interval_year', 'union'],
  hive: ['fixed', 'time', 'timestamp_tz', 'uuid'],
  'jdbc-mysql': [
    'boolean',
    'fixed',
    'struct',
    'list',
    'map',
    'timestamp_tz',
    'interval_day',
    'interval_year',
    'union',
    'uuid'
  ],
  'jdbc-postgresql': ['fixed', 'byte', 'struct', 'map', 'interval_day', 'interval_year', 'union', 'uuid'],
  'jdbc-doris': [
    'fixed',
    'struct',
    'list',
    'map',
    'timestamp_tz',
    'interval_day',
    'interval_year',
    'union',
    'uuid',
    'binary',
    'time'
  ],
  'lakehouse-generic': ['char', 'varchar', 'time', 'timestamp_tz'],
  'lakehouse-paimon': ['interval_day', 'interval_year', 'union', 'uuid'],
  'jdbc-clickhouse': [
    'binary',
    'fixed',
    'struct',
    'list',
    'map',
    'interval_day',
    'interval_year',
    'union',
    'time',
    'timestamp_tz'
  ]
}

const tableLevelPropInfoMap = {
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
    allowAdd: true
  },
  'jdbc-mysql': {
    reserved: [],
    immutable: ['auto-increment-offset', 'engine'],
    allowAdd: true
  },
  'jdbc-oceanbase': {
    reserved: [],
    immutable: [],
    allowAdd: false
  },
  starrocks: {
    reserved: [],
    immutable: [],
    allowAdd: true
  },
  'jdbc-postgresql': {
    reserved: [],
    immutable: [],
    allowAdd: false
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
    allowAdd: true
  },
  'lakehouse-paimon': {
    reserved: ['bucket-key', 'comment', 'owner', 'partition', 'primary-key'],
    immutable: ['merge-engine', 'rowkind.field', 'sequence.field'],
    allowAdd: true
  },
  kafka: {
    reserved: [],
    immutable: ['replication-factor'],
    allowAdd: true
  },
  fileset: {
    reserved: [],
    immutable: ['default-location-name'],
    allowAdd: true
  }
}

export const getPropInfo = provider => {
  if (Object.hasOwn(tableLevelPropInfoMap, provider)) {
    return tableLevelPropInfoMap[provider]
  }

  return {
    reserved: [],
    immutable: ['in-use'],
    allowAdd: true
  }
}

export const distributionInfoMap = {
  hive: {
    required: false,
    strategyOption: ['hash'],
    funcArgs: ['field']
  },
  'jdbc-doris': {
    required: true,
    strategyOption: ['hash', 'even'],
    funcArgs: ['field']
  },
  'jdbc-starrocks': {
    required: true,
    strategyOption: ['hash', 'even'],
    funcArgs: ['field']
  },
  'lakehouse-iceberg': {
    required: false,
    strategyOption: ['hash', 'range'],
    funcArgs: null
  }
}

export const partitionInfoMap = {
  hive: ['identity'],
  'jdbc-doris': ['range', 'list'],
  'lakehouse-iceberg': ['identity', 'bucket', 'truncate', 'year', 'month', 'day', 'hour'],
  'lakehouse-paimon': ['identity']
}

export const transformsLimitMap = {
  bucket: [
    'integer',
    'long',
    'decimal',
    'date',
    'time',
    'timestamp',
    'timestamp_tz',
    'string',
    'uuid',
    'fixed',
    'binary'
  ],
  truncate: ['integer', 'long', 'decimal', 'string', 'binary'],
  year: ['date', 'timestamp', 'timestamp_tz'],
  month: ['date', 'timestamp', 'timestamp_tz'],
  day: ['date', 'timestamp', 'timestamp_tz'],
  hour: ['timestamp', 'timestamp_tz']
}

export const sortOrdersInfoMap = {
  hive: ['field'],
  'lakehouse-iceberg': ['field', 'bucket', 'truncate', 'year', 'month', 'day', 'hour']
}

export const indexesInfoMap = {
  'jdbc-doris': ['primary_key'],
  'jdbc-starrocks': ['primary_key'],
  'jdbc-mysql': ['primary_key', 'unique_key'],
  'jdbc-oceanbase': ['primary_key', 'unique_key'],
  'jdbc-postgresql': ['primary_key', 'unique_key'],
  'lakehouse-paimon': ['primary_key']
}

export const autoIncrementInfoMap = {
  'jdbc-mysql': {
    notNullCheck: true,
    uniqueCheck: true
  },
  'jdbc-oceanbase': {
    notNullCheck: true,
    uniqueCheck: true
  },
  'jdbc-postgresql': {
    notNullCheck: true,
    uniqueCheck: false
  }
}

export const defaultValueSupported = ['jdbc-doris', 'jdbc-mysql', 'jdbc-oceanbase', 'jdbc-postgresql', 'jdbc-starrocks']
