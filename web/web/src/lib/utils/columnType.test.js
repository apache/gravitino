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

import { describe, expect, it } from 'vitest'
import { formatColumnType } from './columnType'

describe('formatColumnType', () => {
  it.each([
    ['a primitive type', 'string', 'string'],
    ['a list', { type: 'list', containsNull: true, elementType: 'string' }, 'list<string>'],
    [
      'a list of non-null elements',
      { type: 'list', containsNull: false, elementType: 'integer' },
      'list<integer, NOT NULL>'
    ],
    [
      'a map of lists',
      {
        type: 'map',
        keyType: 'string',
        valueContainsNull: true,
        valueType: { type: 'list', containsNull: true, elementType: 'long' }
      },
      'map<string,list<long>>'
    ],
    [
      'a struct',
      {
        type: 'struct',
        fields: [
          { name: 'id', type: 'long', nullable: false },
          { name: 'tags', type: { type: 'list', containsNull: true, elementType: 'string' }, nullable: true }
        ]
      },
      'struct<id: long NOT NULL,tags: list<string>>'
    ],
    ['a union', { type: 'union', types: ['integer', 'string'] }, 'union<integer,string>'],
    ['an external type', { type: 'external', catalogString: 'FixedSizeList(4)' }, 'external(FixedSizeList(4))'],
    ['an unparsed type', { type: 'unparsed', unparsedType: 'geo' }, 'unparsed(geo)'],
    ['a missing type', undefined, '']
  ])('formats %s', (_, type, expected) => {
    expect(formatColumnType(type)).toBe(expected)
  })
})
