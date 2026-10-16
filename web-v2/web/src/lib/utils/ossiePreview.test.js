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
import { parseOssiePreview } from './ossiePreview'

describe('OSSIE preview', () => {
  it.each(['json', 'yaml'])('counts nested fields and metadata in %s', format => {
    const json =
      '{"version":"0.2.0","name":"reporting","datasets":[{"fields":[{},{}]},{"fields":[{}]}],"relationships":[{}],"metrics":[{},{}]}'

    const yaml =
      'version: 0.2.0\nname: reporting\ndatasets:\n  - fields: [{}, {}]\n  - fields: [{}]\nrelationships: [{}]\nmetrics: [{}, {}]\n'
    expect(parseOssiePreview(format === 'json' ? json : yaml, format)).toEqual({
      name: 'reporting',
      version: '0.2.0',
      datasets: 2,
      fields: 3,
      relationships: 1,
      metrics: 2
    })
  })
  it('allows optional omitted or null collections without rewriting source text', () => {
    expect(parseOssiePreview('version: 0.2.0\nname: model\nmetrics: null', 'yaml')).toEqual({
      name: 'model',
      version: '0.2.0',
      datasets: 0,
      fields: 0,
      relationships: 0,
      metrics: 0
    })
  })
  it.each([
    ['', 'yaml'],
    ['{ invalid }', 'json'],
    ['name: [', 'yaml'],
    ['[]', 'json'],
    ['name: 42', 'yaml'],
    ['version: 0.2.0\nname: model\ndatasets: bad', 'yaml'],
    ['version: 0.2.0\nname: model\ndatasets: [null]', 'yaml'],
    ['version: 0.2.0\nname: model\ndatasets: [{fields: bad}]', 'yaml'],
    ['version: 0.2.0\nname: model\n---\nname: other', 'yaml']
  ])('rejects unparseable or uncountable documents: %s', (text, format) => {
    expect(() => parseOssiePreview(text, format)).toThrow()
  })
})

it.each([undefined, null, 2, {}, [], '', '   '])('rejects invalid OSSIE version %j', version => {
  expect(() => parseOssiePreview(JSON.stringify({ name: 'model', version }), 'json')).toThrow(
    'non-empty string version'
  )
})
