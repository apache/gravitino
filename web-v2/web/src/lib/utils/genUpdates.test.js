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
import { genUpdates, normalizeColumnDefaultValue, isAutoIncrementColumnTypeSupported } from './index'

const createTableData = (autoIncrement, defaultValue) => ({
  name: 'users',
  columns: [
    {
      uniqueId: 'id',
      name: 'id',
      type: 'long',
      nullable: false,
      autoIncrement,
      comment: '',
      ...(defaultValue !== undefined ? { defaultValue } : {})
    }
  ]
})

describe('genUpdates', () => {
  it.each([
    [false, true],
    [true, false]
  ])('generates auto-increment update when changing from %s to %s', (originalValue, newValue) => {
    const originalData = createTableData(originalValue)
    const newData = createTableData(newValue)

    expect(genUpdates(originalData, newData)).toContainEqual({
      '@type': 'updateColumnAutoIncrement',
      fieldName: ['id'],
      autoIncrement: newValue
    })
  })

  it('does not generate an auto-increment update when the value is unchanged', () => {
    const originalData = createTableData(false)
    const newData = createTableData(false)

    expect(genUpdates(originalData, newData)).toEqual([])
  })

  it('preserves REST function defaults without generating unnecessary updates', () => {
    const defaultValue = {
      type: 'function',
      funcName: 'current_timestamp',
      funcArgs: []
    }

    const normalized = normalizeColumnDefaultValue(defaultValue)

    const originalData = createTableData(false, defaultValue)
    const newData = createTableData(false, normalized)

    expect(normalized).toEqual(defaultValue)
    expect(genUpdates(originalData, newData)).toEqual([])
  })

  it('preserves nested function expressions using REST property names', () => {
    const expression = {
      type: 'function',
      funcName: 'coalesce',
      funcArgs: [
        {
          type: 'field',
          fieldName: ['created_at']
        },
        {
          type: 'function',
          funcName: 'lower',
          funcArgs: [
            {
              type: 'literal',
              dataType: 'string',
              value: 'UNKNOWN'
            }
          ]
        }
      ]
    }

    expect(normalizeColumnDefaultValue(expression)).toEqual(expression)
  })

  it('preserves field default expressions', () => {
    const expression = {
      type: 'field',
      fieldName: ['created_at']
    }

    expect(normalizeColumnDefaultValue(expression)).toEqual(expression)
  })

  it('preserves unparsed default expressions', () => {
    const expression = {
      type: 'unparsed',
      unparsedExpression: 'CURRENT_TIMESTAMP'
    }

    expect(normalizeColumnDefaultValue(expression)).toEqual(expression)
  })

  it.each([true, false])('includes autoIncrement when adding a new column: %s', autoIncrement => {
    const originalData = {
      name: 'users',
      columns: []
    }

    const newData = createTableData(autoIncrement)

    expect(genUpdates(originalData, newData)).toContainEqual({
      '@type': 'addColumn',
      fieldName: ['id'],
      type: 'long',
      nullable: false,
      autoIncrement,
      comment: ''
    })
  })

  it.each([undefined, null, '', 'string', 'map'])('safely rejects unsupported or missing column type: %s', type => {
    expect(isAutoIncrementColumnTypeSupported(type)).toBe(false)
  })

  it.each(['long', 'Long', 'LONG', ' Long '])('recognizes supported AutoIncrement type: %s', type => {
    expect(isAutoIncrementColumnTypeSupported(type)).toBe(true)
  })

  it('generates an update when a function default actually changes', () => {
    const originalDefault = {
      type: 'function',
      funcName: 'current_timestamp',
      funcArgs: []
    }

    const newDefault = {
      type: 'function',
      funcName: 'date_trunc',
      funcArgs: [
        {
          type: 'literal',
          dataType: 'string',
          value: 'day'
        }
      ]
    }

    const updates = genUpdates(createTableData(false, originalDefault), createTableData(false, newDefault))

    expect(updates).toContainEqual({
      '@type': 'updateColumnDefaultValue',
      fieldName: ['id'],
      newDefaultValue: newDefault
    })
  })
})
