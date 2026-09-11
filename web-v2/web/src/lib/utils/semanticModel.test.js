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
