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
import { genUpdates } from './index'

const createTableData = autoIncrement => ({
  name: 'users',
  columns: [
    {
      uniqueId: 'id',
      name: 'id',
      type: 'long',
      nullable: false,
      autoIncrement,
      comment: ''
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
})
