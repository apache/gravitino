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

import { describe, expect, test } from 'vitest'
import {
  formatSemanticModelDefinition,
  genSemanticModelUpdates,
  semanticModelErrorMessage,
  parseSemanticModelDefinition,
  validateSemanticModelDefinition
} from './semanticModel'

const orders = {
  name: 'orders',
  source: { namespace: ['sales', 'mart'], name: 'orders' },
  fields: [{ name: 'order_id', expression: { dialects: [{ dialect: 'ANSI_SQL', expression: 'order_id' }] } }]
}

const customers = {
  name: 'customers',
  source: { namespace: ['sales', 'mart'], name: 'customers' }
}

const definitionOf = overrides => ({ datasets: [orders], ...overrides })

describe('validateSemanticModelDefinition', () => {
  test('accepts a minimal definition with a single dataset', () => {
    expect(validateSemanticModelDefinition(definitionOf())).toEqual([])
  })

  test('rejects a definition that is not an object', () => {
    expect(validateSemanticModelDefinition([orders])).toEqual(['The definition must be a JSON object.'])
  })

  test('rejects a definition without datasets', () => {
    expect(validateSemanticModelDefinition({})).toEqual(['The definition must declare at least one dataset.'])
  })

  test('rejects an empty datasets array', () => {
    expect(validateSemanticModelDefinition({ datasets: [] })).toEqual([
      'The definition must declare at least one dataset.'
    ])
  })

  test('rejects a dataset without a name', () => {
    const errors = validateSemanticModelDefinition({ datasets: [{ source: orders.source }] })

    expect(errors).toEqual(['datasets[0] must have a non-empty name.'])
  })

  test('rejects a dataset whose source namespace is not catalog.schema', () => {
    const errors = validateSemanticModelDefinition({
      datasets: [{ name: 'orders', source: { namespace: ['sales'], name: 'orders' } }]
    })

    expect(errors).toEqual(['Dataset "orders" must have a source namespace of exactly [catalog, schema].'])
  })

  test('rejects a dataset source without a name', () => {
    const errors = validateSemanticModelDefinition({
      datasets: [{ name: 'orders', source: { namespace: ['sales', 'mart'] } }]
    })

    expect(errors).toEqual(['Dataset "orders" must have a source with a non-empty name.'])
  })

  test('rejects duplicate dataset names', () => {
    const errors = validateSemanticModelDefinition({ datasets: [orders, orders] })

    expect(errors).toEqual(['Dataset names must be unique within the semantic model: orders.'])
  })

  test.each([null, 'field', {}, { name: '' }, { name: '  ' }, { name: 42 }])('rejects an invalid field %j', field => {
    const errors = validateSemanticModelDefinition({ datasets: [{ ...orders, fields: [field] }] })
    expect(errors).toEqual(['datasets[0].fields[0] must have a non-empty name.'])
  })

  test('reports invalid field names without calling them duplicates', () => {
    const errors = validateSemanticModelDefinition({
      datasets: [{ ...orders, fields: [{}, {}, { name: '' }, { name: '' }, ...orders.fields, ...orders.fields] }]
    })
    expect(errors).toEqual([
      ...[0, 1, 2, 3].map(index => `datasets[0].fields[${index}] must have a non-empty name.`),
      'Field names must be unique within dataset "orders": order_id.'
    ])
  })

  test('rejects duplicate field names within one dataset', () => {
    const errors = validateSemanticModelDefinition({
      datasets: [{ ...orders, fields: [...orders.fields, ...orders.fields] }]
    })

    expect(errors).toEqual(['Field names must be unique within dataset "orders": order_id.'])
  })

  test('accepts the same field name in two different datasets', () => {
    const errors = validateSemanticModelDefinition({
      datasets: [orders, { ...customers, fields: orders.fields }]
    })

    expect(errors).toEqual([])
  })

  test('rejects a relationship endpoint that is not a declared dataset', () => {
    const errors = validateSemanticModelDefinition({
      datasets: [orders],
      relationships: [
        { name: 'orders_customers', from: 'orders', to: 'customers', fromColumns: ['a'], toColumns: ['b'] }
      ]
    })

    expect(errors).toEqual(['Relationship "orders_customers" references an unknown dataset "customers".'])
  })

  test('rejects a relationship whose column counts do not match', () => {
    const errors = validateSemanticModelDefinition({
      datasets: [orders, customers],
      relationships: [
        { name: 'orders_customers', from: 'orders', to: 'customers', fromColumns: ['a'], toColumns: ['b', 'c'] }
      ]
    })

    expect(errors).toEqual(['Relationship "orders_customers" must have the same number of fromColumns and toColumns.'])
  })

  test('rejects a relationship with empty columns', () => {
    const errors = validateSemanticModelDefinition({
      datasets: [orders, customers],
      relationships: [{ name: 'orders_customers', from: 'orders', to: 'customers', fromColumns: [], toColumns: [] }]
    })

    expect(errors).toEqual(['Relationship "orders_customers" must have non-empty fromColumns and toColumns.'])
  })

  test('rejects duplicate relationship names', () => {
    const relationship = {
      name: 'orders_customers',
      from: 'orders',
      to: 'customers',
      fromColumns: ['customer_id'],
      toColumns: ['id']
    }

    const errors = validateSemanticModelDefinition({
      datasets: [orders, customers],
      relationships: [relationship, relationship]
    })

    expect(errors).toEqual(['Relationship names must be unique within the semantic model: orders_customers.'])
  })

  test('rejects duplicate metric names', () => {
    const metric = { name: 'revenue', expression: { dialects: [{ dialect: 'ANSI_SQL', expression: 'sum(amount)' }] } }
    const errors = validateSemanticModelDefinition({ datasets: [orders], metrics: [metric, metric] })

    expect(errors).toEqual(['Metric names must be unique within the semantic model: revenue.'])
  })

  test('reports every problem it finds rather than only the first', () => {
    const errors = validateSemanticModelDefinition({
      datasets: [{ source: orders.source }, { name: 'customers' }]
    })

    expect(errors).toEqual([
      'datasets[0] must have a non-empty name.',
      'Dataset "customers" must have a source with a namespace and a name.'
    ])
  })
})

describe('nested Semantic Model contract validation', () => {
  const expression = { dialects: [{ dialect: 'ANSI_SQL', expression: 'amount' }] }
  const relationship = { name: 'self', from: 'orders', to: 'orders', fromColumns: ['id'], toColumns: ['id'] }

  test.each(['field', 'metric'])('accepts only the server-supported optional %s data types', kind => {
    const withType = datatype =>
      kind === 'field'
        ? { datasets: [{ ...orders, fields: [{ ...orders.fields[0], datatype }] }] }
        : definitionOf({ metrics: [{ name: 'revenue', expression, datatype }] })
    for (const datatype of [
      undefined,
      null,
      'String',
      'Integer',
      'Decimal',
      'Float',
      'Boolean',
      'Date',
      'Time',
      'DateTime',
      'DateTimeTz',
      'Opaque'
    ]) {
      expect(validateSemanticModelDefinition(withType(datatype))).toEqual([])
    }
    for (const datatype of ['', ' ', 'decimal', 'Decimal(10,2)', 'Unsupported', 42, {}, []]) {
      const { definition, errors } = parseSemanticModelDefinition(JSON.stringify(withType(datatype)))
      expect(definition).toBeNull()
      expect(errors).toHaveLength(1)
      expect(errors[0]).toContain(kind === 'field' ? 'datasets[0].fields[0].datatype' : 'metrics[0].datatype')
    }
  })

  test.each([
    undefined,
    null,
    {},
    { dialects: [] },
    { dialects: [null] },
    { dialects: [{ dialect: '', expression: 'amount' }] },
    { dialects: [{ dialect: 'ANSI_SQL', expression: '' }] },
    {
      dialects: [
        { dialect: 'ANSI_SQL', expression: 'amount' },
        { dialect: 'ANSI_SQL', expression: 'amount + 1' }
      ]
    }
  ])('rejects malformed field and metric expressions: %j', expression => {
    for (const definition of [
      { datasets: [{ ...orders, fields: [{ name: 'amount', expression }] }] },
      definitionOf({ metrics: [{ name: 'revenue', expression }] })
    ]) {
      expect(validateSemanticModelDefinition(definition).join(' ')).toMatch(/expression|dialect/)
    }
  })

  test.each(['', ' ', null, 42])('rejects invalid relationship column %j on either side', column => {
    for (const side of ['fromColumns', 'toColumns']) {
      const errors = validateSemanticModelDefinition(
        definitionOf({ relationships: [{ ...relationship, [side]: [column] }] })
      )
      expect(errors.join(' ')).toContain(side)
    }
  })

  test.each([
    { primaryKey: [null] },
    { primaryKey: [''] },
    { primaryKey: 'id' },
    { uniqueKeys: [null] },
    { uniqueKeys: [[]] },
    { uniqueKeys: [['']] },
    { uniqueKeys: [['id', null]] },
    { uniqueKeys: 'id' }
  ])('rejects malformed dataset keys: %j', keys => {
    expect(validateSemanticModelDefinition({ datasets: [{ ...orders, ...keys }] }).join(' ')).toMatch(
      /primaryKey|uniqueKeys/
    )
  })

  const withMetadata = (level, metadata) => {
    if (level === 'model') return definitionOf(metadata)
    if (level === 'dataset') return { datasets: [{ ...orders, ...metadata }] }
    if (level === 'field') return { datasets: [{ ...orders, fields: [{ name: 'amount', expression, ...metadata }] }] }
    if (level === 'relationship') return definitionOf({ relationships: [{ ...relationship, ...metadata }] })

    return definitionOf({ metrics: [{ name: 'revenue', expression, ...metadata }] })
  }

  test.each(['model', 'dataset', 'field', 'relationship', 'metric'])(
    'checks extensions and AI context at %s level',
    level => {
      for (const metadata of [
        { customExtensions: {} },
        { customExtensions: [null] },
        { customExtensions: [{ vendorName: 'vendor' }] },
        { customExtensions: [{ data: '{}' }] },
        { aiContext: [] },
        { aiContext: 42 },
        { aiContext: { synonyms: [null] } },
        { aiContext: { examples: [null] } }
      ]) {
        expect(validateSemanticModelDefinition(withMetadata(level, metadata)).length).toBeGreaterThan(0)
      }
      for (const aiContext of [
        null,
        '',
        {},
        { instructions: 'guide', synonyms: [''], examples: [], extra: { a: 1 } }
      ]) {
        expect(
          validateSemanticModelDefinition(
            withMetadata(level, {
              aiContext,
              customExtensions: [{ vendorName: '', data: '' }]
            })
          )
        ).toEqual([])
      }
    }
  )

  test('allows optional null arrays and valid nested structures', () => {
    expect(
      validateSemanticModelDefinition({
        datasets: [{ ...orders, fields: null, primaryKey: null, uniqueKeys: null }],
        relationships: null,
        metrics: null,
        customExtensions: null
      })
    ).toEqual([])
    expect(
      validateSemanticModelDefinition({
        datasets: [{ ...orders, primaryKey: ['order_id'], uniqueKeys: [['amount'], ['id', 'region']] }],
        relationships: [relationship],
        metrics: [
          {
            name: 'revenue',
            expression: { dialects: [...expression.dialects, { dialect: 'SPARK_SQL', expression: 'sum(amount)' }] }
          }
        ]
      })
    ).toEqual([])
  })
})

describe('parseSemanticModelDefinition', () => {
  test('returns the parsed definition when the text is valid', () => {
    const { definition, errors } = parseSemanticModelDefinition(JSON.stringify(definitionOf()))

    expect(errors).toEqual([])
    expect(definition).toEqual(definitionOf())
  })

  test('reports a parse failure without throwing', () => {
    const { definition, errors } = parseSemanticModelDefinition('{ not json }')

    expect(definition).toBeNull()
    expect(errors).toHaveLength(1)
    expect(errors[0]).toMatch(/^The definition is not valid JSON/)
  })

  test('reports a blank definition', () => {
    expect(parseSemanticModelDefinition('   ')).toEqual({
      definition: null,
      errors: ['The definition is required.']
    })
  })

  test('reports contract errors for well-formed JSON', () => {
    const { definition, errors } = parseSemanticModelDefinition('{"datasets": []}')

    expect(definition).toBeNull()
    expect(errors).toEqual(['The definition must declare at least one dataset.'])
  })
})

describe('formatSemanticModelDefinition', () => {
  test('pretty-prints a definition', () => {
    expect(formatSemanticModelDefinition({ datasets: [] })).toBe('{\n  "datasets": []\n}')
  })

  test('returns an empty string for a missing definition', () => {
    expect(formatSemanticModelDefinition(null)).toBe('')
  })
})

describe('genSemanticModelUpdates', () => {
  const original = {
    name: 'sales_model',
    comment: 'Governed sales definitions',
    properties: { domain: 'sales' },
    definition: definitionOf()
  }

  test('returns no updates when nothing changed', () => {
    expect(genSemanticModelUpdates(original, { ...original })).toEqual([])
  })

  test('emits a rename when the name changed', () => {
    expect(genSemanticModelUpdates(original, { ...original, name: 'sales_model_v2' })).toEqual([
      { '@type': 'rename', newName: 'sales_model_v2' }
    ])
  })

  test('emits an updateComment when the comment changed', () => {
    expect(genSemanticModelUpdates(original, { ...original, comment: 'Updated' })).toEqual([
      { '@type': 'updateComment', newComment: 'Updated' }
    ])
  })

  test('clears the comment with a null newComment', () => {
    expect(genSemanticModelUpdates(original, { ...original, comment: '' })).toEqual([
      { '@type': 'updateComment', newComment: null }
    ])
  })

  test('emits removeProperty before setProperty', () => {
    const updates = genSemanticModelUpdates(original, { ...original, properties: { region: 'emea' } })

    expect(updates).toEqual([
      { '@type': 'removeProperty', property: 'domain' },
      { '@type': 'setProperty', property: 'region', value: 'emea' }
    ])
  })

  test.each(['constructor', 'toString', '__proto__'])('removes the own property %s', key => {
    expect(
      genSemanticModelUpdates({ ...original, properties: { [key]: 'value' } }, { ...original, properties: {} })
    ).toEqual([{ '@type': 'removeProperty', property: key }])
  })

  test('ignores inherited properties', () => {
    expect(
      genSemanticModelUpdates(
        { ...original, properties: Object.create({ old: 'value' }) },
        { ...original, properties: Object.create({ next: 'value' }) }
      )
    ).toEqual([])
  })

  test('emits setProperty only for values that actually changed', () => {
    const updates = genSemanticModelUpdates(original, {
      ...original,
      properties: { domain: 'sales', region: 'emea' }
    })

    expect(updates).toEqual([{ '@type': 'setProperty', property: 'region', value: 'emea' }])
  })

  test('emits replaceDefinition when the definition changed', () => {
    const next = definitionOf({ datasets: [orders, customers] })
    const updates = genSemanticModelUpdates(original, { ...original, definition: next })

    expect(updates).toEqual([{ '@type': 'replaceDefinition', definition: next }])
  })

  test('does not emit replaceDefinition when the definition is deeply equal but not identical', () => {
    const updates = genSemanticModelUpdates(original, {
      ...original,
      definition: JSON.parse(JSON.stringify(original.definition))
    })

    expect(updates).toEqual([])
  })

  test('orders a full batch as rename, comment, properties, then definition', () => {
    const next = definitionOf({ datasets: [orders, customers] })

    const updates = genSemanticModelUpdates(original, {
      name: 'sales_model_v2',
      comment: 'Updated',
      properties: { region: 'emea' },
      definition: next
    })

    expect(updates.map(update => update['@type'])).toEqual([
      'rename',
      'updateComment',
      'removeProperty',
      'setProperty',
      'replaceDefinition'
    ])
  })
})

describe('semanticModelErrorMessage', () => {
  test.each([
    [{ response: { data: { error: { message: 'Invalid source' } } } }, 'Invalid source'],
    [{ response: { data: '{"message":"Model not found"}' } }, 'Model not found'],
    [{ message: 'Network Error' }, 'Network Error'],
    [
      { response: { data: '<html>Proxy error</html>' }, message: 'Request failed with status code 502' },
      'Request failed with status code 502'
    ],
    [null, 'Import failed']
  ])('preserves a readable API or transport error', (error, message) => {
    expect(semanticModelErrorMessage(error, 'Import failed')).toBe(message)
  })
})
