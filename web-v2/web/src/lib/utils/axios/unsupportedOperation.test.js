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
import { isUnsupportedOperationError } from '@/lib/utils/axios/unsupportedOperation'

describe('isUnsupportedOperationError', () => {
  it('recognizes HTTP 501 as an unsupported operation', () => {
    expect(isUnsupportedOperationError({ response: { status: 501 } })).toBe(true)
  })

  it('recognizes the legacy HTTP 405 response by its application error code', () => {
    expect(isUnsupportedOperationError({ response: { status: 405, data: { code: 1006 } } })).toBe(true)
  })

  it('does not hide an actual HTTP method mismatch', () => {
    expect(isUnsupportedOperationError({ response: { status: 405, data: { code: 1000 } } })).toBe(false)
  })

  it('does not treat an HTTP conflict as an unsupported operation', () => {
    expect(isUnsupportedOperationError({ response: { status: 409, data: { code: 1006 } } })).toBe(false)
  })

  it('handles errors without an HTTP response', () => {
    expect(isUnsupportedOperationError(new Error('network error'))).toBe(false)
  })
})
