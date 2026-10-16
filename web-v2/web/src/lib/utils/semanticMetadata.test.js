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
import { getSemanticMetadataEntity, semanticMetadataFormValues, updateSemanticMetadata } from './semanticMetadata'

const metadata = {
  aiContext: { instructions: 'Original', synonyms: ['one'], examples: ['question'], extra: { keep: true } },
  customExtensions: [{ vendorName: 'vendor', data: 'keep exactly' }]
}

const expression = {
  dialects: [
    { dialect: 'ANSI_SQL', expression: '-- comment\nSUM(amount)' },
    { dialect: 'SPARK_SQL', expression: 'SUM(amount)' }
  ]
}

const makeDefinition = () => ({
  ...structuredClone(metadata),
  datasets: [
    {
      name: 'orders',
      description: 'Orders',
      ...structuredClone(metadata),
      source: { namespace: ['sales', 'mart'], name: 'orders' },
      primaryKey: ['id'],
      uniqueKeys: [['amount']],
      fields: [{ name: 'amount', label: 'Amount', expression, ...structuredClone(metadata) }]
    },
    { name: 'other', source: { namespace: ['sales', 'mart'], name: 'other' }, fields: [{ name: 'amount', expression }] }
  ],
  relationships: [
    {
      name: 'join',
      from: 'orders',
      to: 'other',
      fromColumns: ['amount'],
      toColumns: ['amount'],
      ...structuredClone(metadata)
    }
  ],
  metrics: [{ name: 'revenue', description: 'Revenue', expression, ...structuredClone(metadata) }]
})

const targets = [
  { kind: 'model' },
  { kind: 'dataset', name: 'orders' },
  { kind: 'field', name: 'amount', datasetName: 'orders' },
  { kind: 'relationship', name: 'join' },
  { kind: 'metric', name: 'revenue' }
]

describe('focused metadata updates', () => {
  it.each(targets)('preserves every untouched value when editing $kind guidance', target => {
    const definition = makeDefinition()
    const original = structuredClone(definition)
    const initial = semanticMetadataFormValues(getSemanticMetadataEntity(definition, target))
    const updated = updateSemanticMetadata(definition, target, { ...initial, instructions: 'New guidance' })
    const expected = structuredClone(definition)
    getSemanticMetadataEntity(expected, target).aiContext.instructions = 'New guidance'
    expect(updated).toEqual(expected)
    expect(definition).toEqual(original)
    expect(updateSemanticMetadata(definition, target, initial)).toEqual(definition)
  })

  it('edits only the selected field even when another dataset has the same field name', () => {
    const definition = makeDefinition()
    const target = targets[2]
    const values = semanticMetadataFormValues(getSemanticMetadataEntity(definition, target))
    values.label = 'Order amount'
    values.description = 'Updated description'
    values.dialects[0].expression = '-- new comment\nSUM(amount * 2)'
    values.examples.push('Another question')
    const updated = updateSemanticMetadata(definition, target, values)
    expect(updated.datasets[1]).toEqual(definition.datasets[1])
    expect(updated.datasets[0].fields[0]).toEqual({
      ...definition.datasets[0].fields[0],
      label: values.label,
      description: values.description,
      expression: { dialects: values.dialects },
      aiContext: { ...metadata.aiContext, examples: values.examples }
    })
    expect(definition.datasets[0].fields[0].expression.dialects[0].expression).toBe('-- comment\nSUM(amount)')
  })

  it('supports text guidance and conversion to structured guidance without losing the text', () => {
    const definition = makeDefinition()
    definition.aiContext = 'Text guidance'
    const values = semanticMetadataFormValues(definition)
    expect(updateSemanticMetadata(definition, targets[0], { ...values, contextText: 'Updated' }).aiContext).toBe(
      'Updated'
    )
    expect(
      updateSemanticMetadata(definition, targets[0], { ...values, contextFormat: 'structured' }).aiContext
    ).toEqual({ instructions: 'Text guidance' })
  })

  it.each([targets[2], targets[4]])('clears an optional $kind data type without changing other metadata', target => {
    for (const cleared of ['', undefined, null]) {
      const definition = makeDefinition()
      const entity = getSemanticMetadataEntity(definition, target)
      entity.datatype = 'Decimal'
      const values = { ...semanticMetadataFormValues(entity), datatype: cleared }
      const expected = structuredClone(definition)
      getSemanticMetadataEntity(expected, target).datatype = null
      expect(updateSemanticMetadata(definition, target, values)).toEqual(expected)
      expect(entity.datatype).toBe('Decimal')
    }
  })

  it('adds context only when the guidance is edited', () => {
    const definition = makeDefinition()
    delete definition.aiContext
    const values = semanticMetadataFormValues(definition)
    expect(updateSemanticMetadata(definition, targets[0], values)).not.toHaveProperty('aiContext')
    expect(updateSemanticMetadata(definition, targets[0], { ...values, instructions: 'New' }).aiContext).toEqual({
      instructions: 'New'
    })
  })

  it('preserves additional context keys even if a text edit is applied to structured context', () => {
    const definition = makeDefinition()

    const updated = updateSemanticMetadata(definition, targets[0], {
      ...semanticMetadataFormValues(definition),
      contextFormat: 'text',
      contextText: 'New'
    })
    expect(updated.aiContext).toEqual({ ...metadata.aiContext, instructions: 'New' })
  })

  it('rejects a missing target instead of updating another entity', () => {
    expect(() => updateSemanticMetadata(makeDefinition(), { kind: 'metric', name: 'missing' }, {})).toThrow(
      'no longer exists'
    )
  })

  it.each([true, false, undefined])('edits or clears the time dimension without losing other metadata: %s', value => {
    const definition = makeDefinition()
    const target = targets[2]
    const field = getSemanticMetadataEntity(definition, target)
    field.dimension = { isTime: !value, extra: 'preserve' }
    const initial = semanticMetadataFormValues(field)

    const updated = getSemanticMetadataEntity(
      updateSemanticMetadata(definition, target, { ...initial, timeDimension: value }),
      target
    )
    expect(updated.dimension).toEqual(value == null ? { extra: 'preserve' } : { isTime: value, extra: 'preserve' })
    expect(updated.expression).toEqual(field.expression)
  })

  it('leaves an absent dimension alone and removes an explicitly cleared dimension', () => {
    const definition = makeDefinition()
    const target = targets[2]
    const field = getSemanticMetadataEntity(definition, target)
    expect(updateSemanticMetadata(definition, target, semanticMetadataFormValues(field))).toEqual(definition)
    field.dimension = { isTime: true }

    const updated = updateSemanticMetadata(definition, target, {
      ...semanticMetadataFormValues(field),
      timeDimension: undefined
    })
    expect(getSemanticMetadataEntity(updated, target)).not.toHaveProperty('dimension')
  })

  it.each(['', '   '])('removes cleared optional text metadata: %j', blank => {
    const definition = makeDefinition()
    const target = targets[2]
    const field = getSemanticMetadataEntity(definition, target)
    field.description = 'Description'
    field.aiContext = 'Guidance'
    const values = { ...semanticMetadataFormValues(field), label: blank, description: blank, contextText: blank }
    const updated = getSemanticMetadataEntity(updateSemanticMetadata(definition, target, values), target)
    expect(updated).not.toHaveProperty('label')
    expect(updated).not.toHaveProperty('description')
    expect(updated).not.toHaveProperty('aiContext')
    expect(updated.customExtensions).toEqual(field.customExtensions)
  })

  it.each([true, false])('clears structured guidance while preserving additional keys: %s', extra => {
    const definition = makeDefinition()
    definition.aiContext = {
      instructions: 'Guidance',
      synonyms: ['a'],
      examples: ['b'],
      ...(extra ? { vendor: { keep: true } } : {})
    }

    const updated = updateSemanticMetadata(definition, targets[0], {
      ...semanticMetadataFormValues(definition),
      instructions: '',
      synonyms: [],
      examples: []
    })
    if (extra) expect(updated.aiContext).toEqual({ vendor: { keep: true } })
    else expect(updated).not.toHaveProperty('aiContext')
  })

  it('removes structured context when only unchanged empty lists remain after clearing instructions', () => {
    const definition = makeDefinition()
    definition.aiContext = { instructions: 'Guidance', synonyms: [], examples: [] }

    const updated = updateSemanticMetadata(definition, targets[0], {
      ...semanticMetadataFormValues(definition),
      instructions: ''
    })
    expect(updated).not.toHaveProperty('aiContext')
  })
})
