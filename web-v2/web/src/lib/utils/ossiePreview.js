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

import { load, JSON_SCHEMA } from 'js-yaml'
import { isPlainObject } from 'lodash-es'

/** Reads only the import preview; the original document is sent unchanged for server validation. */
export const parseOssiePreview = (document, format) => {
  if (!document.trim()) throw new Error('The selected file is empty.')
  const model = format === 'json' ? JSON.parse(document) : load(document, { schema: JSON_SCHEMA })
  if (!isPlainObject(model)) throw new Error('The OSSIE document must be an object.')
  if (typeof model.name !== 'string' || !model.name.trim()) throw new Error('The document must declare a model name.')

  if (typeof model.version !== 'string' || !model.version.trim())
    throw new Error('The document must declare a non-empty string version.')

  const count = (values, path) => {
    if (values == null) return 0
    if (!Array.isArray(values)) throw new Error(`${path} must be an array.`)

    return values.length
  }
  const datasets = count(model.datasets, 'datasets')

  const fields = (model.datasets || []).reduce((sum, dataset, index) => {
    if (!isPlainObject(dataset)) throw new Error(`datasets[${index}] must be an object.`)

    return sum + count(dataset.fields, `datasets[${index}].fields`)
  }, 0)

  return {
    name: model.name,
    version: model.version,
    datasets,
    fields,
    relationships: count(model.relationships, 'relationships'),
    metrics: count(model.metrics, 'metrics')
  }
}
