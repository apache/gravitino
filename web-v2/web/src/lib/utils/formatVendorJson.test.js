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

import { expect, it } from 'vitest'
import { formatVendorJson } from './formatVendorJson'

it('preserves large integers, precise decimals, exponents, and negative zero', () => {
  expect(formatVendorJson('{"id":9007199254740993,"threshold":0.10000000000000000001,"large":1e400,"zero":-0}')).toBe(
    '{\n  "id": 9007199254740993,\n  "threshold": 0.10000000000000000001,\n  "large": 1e400,\n  "zero": -0\n}'
  )
})

it('preserves string tokens and duplicate keys while indenting nested structures', () => {
  const raw = String.raw`{"2": [], "1": {}, "2": ["escaped \" quote, { } :", {"unicode":"\u0061"}]}`
  const formatted = formatVendorJson(raw)
  expect(formatted).toBe(String.raw`{
  "2": [],
  "1": {},
  "2": [
    "escaped \" quote, { } :",
    {
      "unicode": "\u0061"
    }
  ]
}`)
  expect(JSON.parse(formatted)).toEqual(JSON.parse(raw))
})

it.each(['text', '{ invalid }', '', 'null', '123', '"text"'])('leaves non-object data raw: %s', raw => {
  expect(formatVendorJson(raw)).toBeNull()
})
