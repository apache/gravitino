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

const withNotNull = (type, nullable) => (nullable === false ? `${type}, NOT NULL` : type)

// The column type like the server's Type.simpleString(), such as list<string>; struct fields omit NULL and COMMENT.
export const formatColumnType = type => {
  if (type === null || type === undefined || typeof type === 'string') {
    return type ?? ''
  }
  switch (type.type) {
    case 'list':
      return `list<${withNotNull(formatColumnType(type.elementType), type.containsNull)}>`
    case 'map':
      return `map<${formatColumnType(type.keyType)},${withNotNull(formatColumnType(type.valueType), type.valueContainsNull)}>`
    case 'struct':
      return `struct<${(type.fields || [])
        .map(f => `${f.name}: ${formatColumnType(f.type)}${f.nullable === false ? ' NOT NULL' : ''}`)
        .join(',')}>`
    case 'union':
      return `union<${(type.types || []).map(formatColumnType).join(',')}>`
    case 'external':
      return `external(${type.catalogString})`
    case 'unparsed':
      return `unparsed(${type.unparsedType})`
    default:
      return `${type.type}`
  }
}
