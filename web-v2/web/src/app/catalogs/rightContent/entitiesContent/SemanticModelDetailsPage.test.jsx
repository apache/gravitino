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

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act } from 'react'
import { Simulate } from 'react-dom/test-utils'
import { createRoot } from 'react-dom/client'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import reducer, {
  getSemanticModelDetails,
  getTableDetails,
  resetActivatedDetails,
  getCurrentEntityPolicies
} from '@/lib/store/metalakes'
import SemanticModelDetailsPage from './SemanticModelDetailsPage'
import SemanticMetadataEditor from '../SemanticMetadataEditor'

const navigation = vi.hoisted(() => ({ push: vi.fn(), replace: vi.fn(), name: 'sales model' }))

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), put: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))
vi.mock('next/navigation', () => ({
  useRouter: () => navigation,
  useSearchParams: () =>
    new URLSearchParams({
      metalake: 'demo',
      catalog: 'sales',
      schema: 'mart',
      semanticModel: navigation.name,
      catalogType: 'relational'
    })
}))
vi.mock('@/components/GetOwner', () => ({ default: () => null }))
vi.mock('@/components/CustomTags', () => ({ default: () => null }))

let root
let container
let store
let downloads

beforeEach(() => {
  vi.clearAllMocks()
  navigation.name = 'sales model'
  vi.stubGlobal('IS_REACT_ACT_ENVIRONMENT', true)
  const getComputedStyle = window.getComputedStyle

  // jsdom has no pseudo-element layout; Ant Design uses this to measure scrollbars.
  vi.spyOn(window, 'getComputedStyle').mockImplementation(element => getComputedStyle(element))
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    }
  )
  Object.defineProperty(window, 'matchMedia', {
    configurable: true,
    value: vi.fn(() => ({ matches: false, addListener() {}, removeListener() {} }))
  })
  downloads = []
  vi.stubGlobal(
    'URL',
    class extends URL {
      static createObjectURL = vi.fn(() => 'blob:ossie')
      static revokeObjectURL = vi.fn()
    }
  )
  vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function () {
    downloads.push({ href: this.href, filename: this.download })
  })
  store = configureStore({ reducer: { metalakes: reducer, auth: () => ({ anthEnable: false }) } })
  defHttp.get
    .mockReset()
    .mockImplementation(() =>
      Promise.resolve({ semanticModel: store.getState().metalakes.activatedDetails, identifiers: [] })
    )
  defHttp.put.mockReset()
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
})

afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

const definitionWith = context => ({
  aiContext: context('model'),
  datasets: [
    {
      name: 'orders',
      source: { namespace: ['sales', 'mart'], name: 'orders' },
      aiContext: context('dataset'),
      fields: [
        {
          name: 'amount',
          expression: { dialects: [{ dialect: 'ANSI_SQL', expression: 'amount' }] },
          aiContext: context('field')
        }
      ]
    }
  ],
  relationships: [
    {
      name: 'customers',
      from: 'orders',
      to: 'orders',
      fromColumns: ['amount'],
      toColumns: ['amount'],
      aiContext: context('relationship')
    }
  ],
  metrics: [
    {
      name: 'revenue',
      expression: { dialects: [{ dialect: 'ANSI_SQL', expression: 'SUM(amount)' }] },
      aiContext: context('metric')
    }
  ]
})

const renderDetails = async (definition, details = {}) => {
  const args = { init: true, metalake: 'demo', catalog: 'sales', schema: 'mart', semanticModel: navigation.name }
  store.dispatch(getSemanticModelDetails.pending('load', args))
  store.dispatch(
    getSemanticModelDetails.fulfilled(
      { semanticModel: { name: navigation.name, definition, properties: {}, ...details }, init: true },
      'load',
      args
    )
  )
  await act(async () =>
    root.render(
      <Provider store={store}>
        <SemanticModelDetailsPage />
      </Provider>
    )
  )
}

const click = async element => {
  expect(element).toBeTruthy()
  await act(async () => {
    element.click()
    await new Promise(resolve => setTimeout(resolve, 50))
  })
}

const findText = (selector, text) =>
  [...document.querySelectorAll(selector)].find(element => element.textContent === text)
const expandContext = async label => click(findText('.ant-collapse-header-text', label))
const selectTab = async name => click(findText('[role="tab"]', name))

const exportDocument = async format => {
  await click(findText('button', 'Export OSSIE'))
  await click(findText('[role="menuitem"]', `Download ${format.toUpperCase()}`))
}

const readBlob = blob =>
  new Promise(resolve => {
    const reader = new FileReader()
    reader.onload = () => resolve(reader.result)
    reader.readAsText(blob)
  })

describe('Semantic Model AI guidance', () => {
  it.each(['text', 'structured'])('shows %s context next to all five metadata levels', async form => {
    const context = label =>
      form === 'text'
        ? `${label} guidance\nSecond line`
        : {
            instructions: `${label} guidance`,
            synonyms: [`${label} alias`],
            examples: [`${label} example`],
            extra: { scope: label }
          }
    await renderDetails(definitionWith(context))

    const verify = label => {
      expect(document.body.textContent).toContain(`${label} guidance`)
      if (form === 'structured') {
        expect(document.body.textContent).toContain(`${label} alias`)
        expect(document.body.textContent).toContain(`${label} example`)
        expect(document.body.textContent).toContain(`"scope": "${label}"`)
      }
    }
    await selectTab('Overview')
    await expandContext('Model AI Context')
    verify('model')
    await selectTab('Datasets')
    await click(document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon'))
    await click(findText('button', 'View dataset details'))
    await expandContext('orders AI Context')
    verify('dataset')
    await click(document.querySelector('.ant-modal-close'))
    await click(document.querySelector('[aria-label="amount AI Context"]'))
    verify('field')
    expect(document.querySelector('.ant-modal .ant-collapse')).toBeNull()
    await click(document.querySelector('.ant-modal-close'))
    await selectTab('Relationships')
    await click(
      document.querySelector('[data-refer="semantic-model-relationships-grid"] button.ant-table-row-expand-icon')
    )
    await expandContext('customers AI Context')
    verify('relationship')
    await selectTab('Metrics')
    await click(document.querySelector('[data-refer="semantic-model-metrics-grid"] button.ant-table-row-expand-icon'))
    await expandContext('revenue AI Context')
    verify('metric')
    expect(document.querySelector('[data-refer="semantic-model-definition"]')).toBeNull()
  })

  it('supports a dataset with context and no fields', async () => {
    await renderDetails({ datasets: [{ name: 'orders', aiContext: 'Dataset only' }] })
    await click(document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon'))
    await click(findText('button', 'View dataset details'))
    await expandContext('orders AI Context')
    expect(document.body.textContent).toContain('Dataset only')
  })
})

describe('OSSIE downloads', () => {
  it.each([
    ['yaml', 'name: sales model\nai_context: Guide\n'],
    ['json', '{ "name": "sales model", "ai_context": "Guide" }']
  ])('downloads the server %s document unchanged', async (format, content) => {
    defHttp.get.mockResolvedValue(content)
    await renderDetails({ datasets: [] })
    await exportDocument(format)
    expect(defHttp.get).toHaveBeenCalledWith(
      {
        url: '/api/metalakes/demo/catalogs/sales/schemas/mart/semantic-models/sales%20model/ossie',
        params: { format },
        responseType: 'text'
      },
      { errorMessageMode: 'none' }
    )
    expect(downloads).toEqual([{ href: 'blob:ossie', filename: `sales_model.ossie.${format}` }])
    const blob = URL.createObjectURL.mock.calls[0][0]
    expect(blob.type).toBe(format === 'json' ? 'application/json' : 'application/yaml')
    expect(await readBlob(blob)).toBe(content)
    expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:ossie')
  })

  it('shows a server error instead of downloading it and supports retry', async () => {
    defHttp.get
      .mockRejectedValueOnce({ response: { data: '{"message":"Model no longer exists"}' } })
      .mockResolvedValue('name: sales model')
    await renderDetails({ datasets: [] })
    await exportDocument('yaml')
    expect(document.querySelector('.ant-alert-description').textContent).toBe('Model no longer exists')
    expect(downloads).toEqual([])
    await exportDocument('yaml')
    expect(downloads).toHaveLength(1)
    expect(document.querySelector('.ant-alert')).toBeNull()
  })
})

describe('Semantic Model metadata', () => {
  it('keeps composite unique keys grouped and shows labels alongside field names', async () => {
    await renderDetails({
      datasets: [
        {
          name: 'orders',
          uniqueKeys: [['customer_id', 'order_id'], ['external_id']],
          fields: [{ name: 'amount', label: 'Order Amount (USD)' }, { name: 'id' }]
        }
      ]
    })
    const tags = document.querySelectorAll('[data-refer="semantic-model-datasets-grid"] .ant-table-tbody .ant-tag')
    expect([...tags].map(tag => tag.textContent)).toEqual(['customer_id, order_id', 'external_id'])
    await click(document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon'))
    const fields = document.querySelector('[data-refer="semantic-model-fields-grid"]')
    expect(fields.textContent).toContain('amount')
    expect(fields.textContent).toContain('Order Amount (USD)')
    expect(fields.textContent).toContain('id')
    expect(fields.textContent).not.toContain('undefined')
  })

  it('exposes complete vendor extensions at all levels, independently of AI context', async () => {
    const extension = name => [
      { vendorName: `${name} vendor`, data: `line 1\n{"private":"${name}","nested":{"flag":true}}` }
    ]
    const definition = definitionWith(() => undefined)
    definition.customExtensions = extension('model')
    definition.datasets[0].customExtensions = extension('dataset')
    definition.datasets[0].fields[0].customExtensions = extension('field')
    definition.relationships[0].customExtensions = extension('relationship')
    definition.metrics[0].customExtensions = extension('metric')
    await renderDetails(definition)

    const verify = async name => {
      const label = {
        model: 'Model',
        dataset: 'orders',
        field: 'amount',
        relationship: 'customers',
        metric: 'revenue'
      }[name]
      await expandContext(`${label} Vendor Extensions`)
      expect([...document.querySelectorAll('pre')].some(pre => pre.textContent === extension(name)[0].data)).toBe(true)
    }
    await selectTab('Overview')
    await verify('model')
    await selectTab('Datasets')
    await click(document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon'))
    await click(findText('button', 'View dataset details'))
    await verify('dataset')
    await click(document.querySelector('.ant-modal-close'))
    await click(document.querySelector('[aria-label="amount Vendor Extensions"]'))
    await verify('field')
    await click(document.querySelector('.ant-modal-close'))
    await selectTab('Relationships')
    await click(
      document.querySelector('[data-refer="semantic-model-relationships-grid"] button.ant-table-row-expand-icon')
    )
    await verify('relationship')
    await selectTab('Metrics')
    await click(document.querySelector('[data-refer="semantic-model-metrics-grid"] button.ant-table-row-expand-icon'))
    await verify('metric')
  })

  it('displays modification audit values when present', async () => {
    await renderDetails(
      { datasets: [] },
      { audit: { lastModifier: 'editor', lastModifiedTime: '2026-10-13T05:00:00Z' } }
    )
    await selectTab('Overview')
    expect(document.querySelector('[data-refer="semantic-model-last-modifier"]').textContent).toBe('editor')
    expect(document.querySelector('[data-refer="semantic-model-last-modified-time"]').textContent).not.toBe('-')
  })

  it('omits absent modification audit values', async () => {
    await renderDetails({ datasets: [] })
    await selectTab('Overview')
    expect(document.querySelector('[data-refer="semantic-model-last-modifier"]')).toBeNull()
    expect(document.querySelector('[data-refer="semantic-model-last-modified-time"]')).toBeNull()
  })
})

describe('editing from Semantic Model details', () => {
  it.each([false, true])(
    'preserves failed edits, then refreshes or navigates after saving (rename=%s)',
    async rename => {
      navigation.name = 'orders'
      const definition = { datasets: [{ name: 'orders', source: { namespace: ['sales', 'mart'], name: 'orders' } }] }
      let model = { name: 'orders', definition, properties: {}, comment: 'original' }

      const updated = {
        ...model,
        name: rename ? 'orders_v2' : 'orders',
        comment: 'saved comment',
        audit: { lastModifier: 'editor' }
      }
      defHttp.get.mockImplementation(({ url }) =>
        Promise.resolve(url.endsWith('/semantic-models') ? { identifiers: [] } : { semanticModel: model })
      )
      defHttp.put.mockRejectedValueOnce(new Error('Invalid source')).mockImplementation(() => {
        model = updated

        return Promise.resolve({ semanticModel: updated })
      })
      await renderDetails(definition, model)
      await click(document.querySelector('[aria-label="Edit Semantic Model"]'))
      await act(async () => {
        Simulate.change(document.querySelector('[data-refer="semantic-model-comment-field"]'), {
          target: { value: 'saved comment' }
        })
        if (rename)
          Simulate.change(document.querySelector('[data-refer="semantic-model-name-field"]'), {
            target: { value: 'orders_v2' }
          })
      })
      await click(document.querySelector('[data-refer="handle-submit-semantic-model"]'))
      expect(document.querySelector('[data-refer="semantic-model-comment-field"]').value).toBe('saved comment')
      expect(navigation.replace).not.toHaveBeenCalled()
      expect(store.getState().metalakes.activatedDetails.comment).toBe('original')
      await click(document.querySelector('[data-refer="handle-submit-semantic-model"]'))
      expect(defHttp.put).toHaveBeenCalledTimes(2)
      if (rename) {
        const destination = new URL(navigation.replace.mock.calls[0][0], 'http://localhost')
        expect(destination.searchParams.get('semanticModel')).toBe('orders_v2')
        expect(destination.searchParams.get('catalogType')).toBe('relational')
        expect(destination.searchParams.get('schema')).toBe('mart')
      } else {
        expect(navigation.replace).not.toHaveBeenCalled()
        expect(store.getState().metalakes.activatedDetails.comment).toBe('saved comment')
        await selectTab('Overview')
        expect(document.querySelector('[data-refer="semantic-model-last-modifier"]').textContent).toBe('editor')
      }
      expect(document.querySelector('[data-refer="handle-submit-semantic-model"]')).toBeNull()
    }
  )
})

describe('Semantic Model accessible details', () => {
  it.each(['sales%20model', 'margin%'])('preserves the already decoded name %s', async name => {
    navigation.name = name
    await renderDetails({ datasets: [] })
    expect(container.querySelector('h3 span[title]').textContent).toBe(name)
  })

  it('labels the copy action', async () => {
    await renderDetails({ datasets: [] })
    await selectTab('Definition')
    expect(container.querySelector('button[aria-label="Copy definition"]')).not.toBeNull()
  })
})

describe('focused semantic metadata editing', () => {
  const selectOption = async (id, label) => {
    await act(async () =>
      Simulate.mouseDown(document.querySelector(id).closest('.ant-select').querySelector('.ant-select-selector'))
    )
    await click(findText('.ant-select-item-option-content', label))
  }

  it.each(['field', 'metric'])('selects and clears the optional %s data type', async kind => {
    const definition = definitionWith(() => null)
    const entity = kind === 'field' ? definition.datasets[0].fields[0] : definition.metrics[0]
    entity.datatype = 'Decimal'
    await renderDetails(definition)
    if (kind === 'field') {
      await click(
        document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon')
      )
      await click(document.querySelector('[aria-label="Edit field amount"]'))
    } else {
      await selectTab('Metrics')
      await click(document.querySelector('[aria-label="Edit metric revenue"]'))
    }
    await selectOption('#semanticMetadata_datatype', 'DateTimeTz')
    const selector = document.querySelector('#semanticMetadata_datatype').closest('.ant-select')
    expect(selector.textContent).toContain('DateTimeTz')
    const expected = structuredClone(definition)
    const updatedEntity = kind === 'field' ? expected.datasets[0].fields[0] : expected.metrics[0]
    updatedEntity.datatype = 'DateTimeTz'

    // Keep the editor open after a failed request so the same draft can also be cleared.
    defHttp.put.mockRejectedValueOnce(new Error('Save rejected'))
    await click(findText('.ant-modal button', 'Save'))
    expect(defHttp.put.mock.lastCall[0].data).toEqual({
      updates: [{ '@type': 'replaceDefinition', definition: expected }]
    })
    await act(async () => Simulate.mouseDown(selector.querySelector('.ant-select-clear')))
    updatedEntity.datatype = null
    const model = { name: navigation.name, definition: expected, properties: {} }
    defHttp.put.mockResolvedValue({ semanticModel: model })
    defHttp.get
      .mockResolvedValue({ semanticModel: model, identifiers: [] })
      .mockResolvedValueOnce({ semanticModel: { ...model, definition } })
    await click(findText('.ant-modal button', 'Save'))
    expect(defHttp.put.mock.lastCall[0].data).toEqual({
      updates: [{ '@type': 'replaceDefinition', definition: expected }]
    })
    expect(document.querySelector('.ant-modal')).toBeNull()
  })

  it('preserves text and structured drafts through repeated format switches before saving', async () => {
    const definition = definitionWith(() => 'Original guidance')
    await renderDetails(definition)
    await selectTab('Overview')
    await click(findText('button', 'Edit Model AI Context'))
    await act(async () =>
      Simulate.change(document.querySelector('#semanticMetadata_contextText'), {
        target: { value: 'Text draft before conversion' }
      })
    )
    await selectOption('#semanticMetadata_contextFormat', 'Structured')
    expect(document.querySelector('#semanticMetadata_instructions').value).toBe('Text draft before conversion')
    await act(async () =>
      Simulate.change(document.querySelector('#semanticMetadata_instructions'), {
        target: { value: 'Edited structured instructions' }
      })
    )
    await click(findText('.ant-modal button', 'Add example'))
    await act(async () =>
      Simulate.change(document.querySelector('#semanticMetadata_examples_0'), {
        target: { value: 'Keep my example' }
      })
    )
    for (let i = 0; i < 2; i++) {
      await selectOption('#semanticMetadata_contextFormat', 'Text')
      expect(document.querySelector('#semanticMetadata_contextText').value).toBe('Text draft before conversion')
      await selectOption('#semanticMetadata_contextFormat', 'Structured')
      expect(document.querySelector('#semanticMetadata_instructions').value).toBe('Edited structured instructions')
      expect(document.querySelector('#semanticMetadata_examples_0').value).toBe('Keep my example')
    }

    const expected = {
      ...definition,
      aiContext: { instructions: 'Edited structured instructions', examples: ['Keep my example'] }
    }
    const model = { name: navigation.name, definition: expected, properties: {} }
    defHttp.put.mockResolvedValue({ semanticModel: model })
    defHttp.get
      .mockResolvedValue({ semanticModel: model, identifiers: [] })
      .mockResolvedValueOnce({ semanticModel: { ...model, definition } })
    await click(findText('.ant-modal button', 'Save'))
    expect(defHttp.put.mock.lastCall[0].data).toEqual({
      updates: [{ '@type': 'replaceDefinition', definition: expected }]
    })
    expect(document.querySelector('.ant-modal')).toBeNull()
  })

  it.each(['model', 'dataset', 'field', 'relationship', 'metric'])(
    'edits only the selected %s metadata and preserves unknown keys',
    async kind => {
      navigation.name = 'orders'
      const definition = definitionWith(scope => ({ instructions: `${scope} guidance`, extra: { preserve: scope } }))
      const extension = [{ vendorName: 'vendor', data: '{"keep":true}' }]
      definition.customExtensions = extension
      definition.datasets[0].fields[0].customExtensions = extension
      let model = { name: 'orders', definition, properties: {} }
      defHttp.get.mockImplementation(({ url }) =>
        Promise.resolve(url.endsWith('/semantic-models') ? { identifiers: [] } : { semanticModel: model })
      )
      defHttp.put.mockImplementation(({ data }) => {
        model = { ...model, definition: data.updates[0].definition }

        return Promise.resolve({ semanticModel: model })
      })
      await renderDetails(definition, model)
      if (kind === 'model') {
        await selectTab('Overview')
        await click(findText('button', 'Edit Model AI Context'))
      }
      if (kind === 'dataset') await click(document.querySelector('[aria-label="Edit dataset orders"]'))
      if (kind === 'field') {
        await click(
          document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon')
        )
        await click(document.querySelector('[aria-label="Edit field amount"]'))
      }
      if (kind === 'relationship') {
        await selectTab('Relationships')
        await click(document.querySelector('[aria-label="Edit relationship customers"]'))
      }
      if (kind === 'metric') {
        await selectTab('Metrics')
        await click(document.querySelector('[aria-label="Edit metric revenue"]'))
      }
      const expected = structuredClone(definition)

      const entity = {
        model: expected,
        dataset: expected.datasets[0],
        field: expected.datasets[0].fields[0],
        relationship: expected.relationships[0],
        metric: expected.metrics[0]
      }[kind]
      entity.aiContext.instructions = 'Edited guidance'
      await act(async () =>
        Simulate.change(document.querySelector('#semanticMetadata_instructions'), {
          target: { value: 'Edited guidance' }
        })
      )
      if (kind === 'field') {
        entity.label = 'Order Amount'
        entity.description = 'Field description'
        entity.expression.dialects[0].expression = '-- comment\namount * 2'
        await act(async () => {
          Simulate.change(document.querySelector('#semanticMetadata_label'), { target: { value: entity.label } })
          Simulate.change(document.querySelector('#semanticMetadata_description'), {
            target: { value: entity.description }
          })
          Simulate.change(document.querySelector('#semanticMetadata_dialects_0_expression'), {
            target: { value: entity.expression.dialects[0].expression }
          })
        })
        defHttp.put.mockRejectedValueOnce(new Error('Save rejected'))
        await click(findText('.ant-modal button', 'Save'))
        expect(document.querySelector('#semanticMetadata_label').value).toBe('Order Amount')
        expect(document.body.textContent).toContain('Your edits are retained')
      }
      await click(findText('.ant-modal button', 'Save'))
      expect(defHttp.put.mock.lastCall[0].data).toEqual({
        updates: [{ '@type': 'replaceDefinition', definition: expected }]
      })
      expect(store.getState().metalakes.activatedDetails.definition).toEqual(expected)
      expect(document.querySelector('.ant-modal')).toBeNull()
    }
  )

  it('blocks an empty dialect list and retains the editor for correction', async () => {
    await renderDetails(definitionWith(() => null))
    await click(document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon'))
    await click(document.querySelector('[aria-label="Edit field amount"]'))
    await click(document.querySelector('[aria-label="Remove dialect 1"]'))
    await click(findText('.ant-modal button', 'Save'))
    expect(document.body.textContent).toContain('Add at least one dialect expression.')
    expect(defHttp.put).not.toHaveBeenCalled()
    expect(document.querySelector('.ant-modal')).not.toBeNull()
  })

  it('closes an unchanged focused edit without sending a definition replacement', async () => {
    await renderDetails(definitionWith(() => null))
    await selectTab('Overview')
    await click(findText('button', 'Edit Model AI Context'))
    await click(findText('.ant-modal button', 'Save'))
    expect(defHttp.put).not.toHaveBeenCalled()
    expect(document.querySelector('.ant-modal')).toBeNull()
  })

  it('keeps SQL line breaks in bounded code blocks', async () => {
    const definition = definitionWith(() => null)
    const sql = '-- comment\nSELECT ' + 'x'.repeat(200)
    definition.metrics[0].expression.dialects[0].expression = sql
    await renderDetails(definition)
    await selectTab('Metrics')
    const code = document.querySelector('[data-refer="semantic-model-metrics-grid"] pre')
    expect(code.textContent).toBe(sql)
    expect(code.className).toContain('whitespace-pre-wrap')
    expect(code.className).toContain('overflow-hidden')
    await click(document.querySelector('[aria-label="View expression for revenue"]'))
    const fullSql = document.querySelector('.ant-modal pre')
    expect(fullSql.textContent).toBe(sql)
    expect(fullSql.className).toContain('leading-6')
    expect(fullSql.className).toContain('overflow-auto')
  })

  it('does not show an AI Context action for a field without guidance', async () => {
    await renderDetails(definitionWith(() => null))
    await click(document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon'))
    expect(document.querySelector('[aria-label="amount AI Context"]')).toBeNull()
  })

  it('keeps model metadata in Overview while browsing compact expression rows', async () => {
    const definition = definitionWith(() => 'guidance')
    definition.metrics[0].expression.dialects = ['ANSI_SQL', 'TRINO', 'SPARK_SQL', 'POSTGRESQL'].map(dialect => ({
      dialect,
      expression: `-- ${dialect}\nSUM(amount)`
    }))
    await renderDetails(definition)
    expect(document.body.textContent).not.toContain('Model AI Context')
    await selectTab('Metrics')
    const grid = document.querySelector('[data-refer="semantic-model-metrics-grid"]')
    expect(grid.querySelectorAll('pre')).toHaveLength(1)
    expect(grid.textContent).toContain('4 dialects')
    await click(document.querySelector('[aria-label="View expression for revenue"]'))
    expect(document.querySelector('.ant-modal pre').textContent).toBe('-- ANSI_SQL\nSUM(amount)')
    await selectOption('[aria-label="SQL dialect"]', 'TRINO')
    expect(document.querySelector('.ant-modal pre').textContent).toBe('-- TRINO\nSUM(amount)')
    await selectOption('[aria-label="SQL dialect"]', 'All dialects')
    expect(document.querySelectorAll('.ant-modal pre')).toHaveLength(4)
    await click(document.querySelector('.ant-modal-close'))
    await selectTab('Overview')
    expect(document.body.textContent).toContain('Model AI Context')
  })

  it('filters fields by label, expands matching datasets, and clears both detail filters', async () => {
    const definition = definitionWith(() => null)
    definition.datasets[0].fields[0].label = 'Revenue amount'
    definition.datasets.push({ name: 'other', fields: [{ name: 'cost' }] })
    definition.metrics.push({ name: 'profit' })
    await renderDetails(definition)
    await act(async () =>
      Simulate.change(document.querySelector('[aria-label="Search fields"]'), { target: { value: 'REVENUE' } })
    )
    const grid = document.querySelector('[data-refer="semantic-model-datasets-grid"]')
    expect(grid.textContent).toContain('Revenue amount')
    expect(grid.textContent).not.toContain('other')
    expect(grid.querySelectorAll('[data-refer="semantic-model-fields-grid"]')).toHaveLength(1)
    await click(findText('button', 'Clear field search'))
    expect(grid.textContent).toContain('other')
    await selectTab('Metrics')
    await act(async () =>
      Simulate.change(document.querySelector('[aria-label="Search metrics"]'), { target: { value: 'PROF' } })
    )
    const metrics = document.querySelector('[data-refer="semantic-model-metrics-grid"]')
    expect(metrics.textContent).toContain('profit')
    expect(metrics.textContent).not.toContain('revenue')
    await click(findText('button', 'Clear metric search'))
    expect(metrics.textContent).toContain('revenue')
  })

  it('uses the existing type palette in fields and metrics with a neutral fallback', async () => {
    const definition = definitionWith(() => null)
    definition.datasets[0].fields[0].datatype = 'String'
    definition.metrics[0].datatype = 'Decimal'
    definition.metrics.push({ name: 'unknown', datatype: 'FutureType' })
    await renderDetails(definition)
    await click(document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon'))
    expect(findText('.ant-tag', 'String').className).toContain('ant-tag-orange')
    await selectTab('Metrics')
    expect(findText('.ant-tag', 'Decimal').className).toContain('ant-tag-blue')
    expect(findText('.ant-tag', 'FutureType').className).not.toMatch(/ant-tag-(blue|orange)/)
  })

  it('formats structured extension data and still exposes the exact raw value', async () => {
    const definition = definitionWith(() => null)
    const raw = '{ "nested": {"flag":true}, "values":[1,2] }'
    definition.customExtensions = [
      { vendorName: 'json', data: raw },
      { vendorName: 'text', data: 'arbitrary text' }
    ]
    await renderDetails(definition)
    await selectTab('Overview')
    await expandContext('Model Vendor Extensions')
    const json = document.querySelector('section[aria-label="json"]')
    expect(json.querySelector('pre').textContent).toBe(JSON.stringify(JSON.parse(raw), null, 2))
    await click(findText('button', 'Show raw value'))
    expect(json.querySelector('pre').textContent).toBe(raw)
    expect(document.querySelector('section[aria-label="text"] pre').textContent).toBe('arbitrary text')
    expect(definition.customExtensions[0].data).toBe(raw)
  })

  it('opens field guidance editing directly and retains drafts across all editor tabs', async () => {
    const definition = definitionWith(() => ({ instructions: 'Original' }))
    await renderDetails(definition)
    await click(document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon'))
    await click(document.querySelector('[aria-label="amount AI Context"]'))
    await click(findText('.ant-modal button', 'Edit AI Context'))
    expect(findText('.ant-modal [role="tab"]', 'AI Context').getAttribute('aria-selected')).toBe('true')
    await act(async () =>
      Simulate.change(document.querySelector('#semanticMetadata_instructions'), {
        target: { value: 'Keep guidance draft' }
      })
    )
    await click(findText('.ant-modal [role="tab"]', 'Expressions'))
    await act(async () =>
      Simulate.change(document.querySelector('#semanticMetadata_dialects_0_expression'), {
        target: { value: '-- draft\namount * 2' }
      })
    )
    await click(findText('.ant-modal [role="tab"]', 'Basic information'))
    await act(async () =>
      Simulate.change(document.querySelector('#semanticMetadata_description'), {
        target: { value: 'Keep description draft' }
      })
    )
    await click(findText('.ant-modal button', 'Cancel'))
    expect(document.body.textContent).toContain('Discard unsaved changes?')
    await click(findText('.ant-modal button', 'Keep editing'))
    expect(document.querySelector('#semanticMetadata_description').value).toBe('Keep description draft')
    await click(findText('.ant-modal [role="tab"]', 'Expressions'))
    expect(document.querySelector('#semanticMetadata_dialects_0_expression').value).toBe('-- draft\namount * 2')
    await click(findText('.ant-modal [role="tab"]', 'AI Context'))
    expect(document.querySelector('#semanticMetadata_instructions').value).toBe('Keep guidance draft')
    await click(findText('.ant-modal button', 'Cancel'))
    await click(findText('.ant-modal button', 'Discard changes'))
    expect(document.querySelector('.ant-modal')).toBeNull()
    expect(defHttp.put).not.toHaveBeenCalled()
  })

  it('closes an unchanged editor immediately on Cancel', async () => {
    await renderDetails(definitionWith(() => null))
    await click(document.querySelector('[aria-label="Edit dataset orders"]'))
    await click(findText('.ant-modal button', 'Cancel'))
    expect(document.querySelector('.ant-modal')).toBeNull()
    expect(document.body.textContent).not.toContain('Discard unsaved changes?')
  })

  it('reveals and focuses invalid SQL even when saving from another section', async () => {
    await renderDetails(definitionWith(() => null))
    await selectTab('Metrics')
    await click(document.querySelector('[aria-label="Edit metric revenue"]'))
    await click(findText('.ant-modal [role="tab"]', 'Expressions'))
    await act(async () =>
      Simulate.change(document.querySelector('#semanticMetadata_dialects_0_expression'), { target: { value: '' } })
    )
    await click(findText('.ant-modal [role="tab"]', 'Basic information'))
    await click(findText('.ant-modal button', 'Save'))
    expect(findText('.ant-modal [role="tab"]', 'Expressions').getAttribute('aria-selected')).toBe('true')
    await act(async () => new Promise(resolve => requestAnimationFrame(resolve)))
    expect(document.activeElement.id).toBe('semanticMetadata_dialects_0_expression')
    expect(document.querySelector('.ant-form-item-explain-error').textContent).toContain('required')
    expect(defHttp.put).not.toHaveBeenCalled()
  })

  it('keeps the server permission reason beside the unsaved draft', async () => {
    await renderDetails(definitionWith(() => null))
    await click(document.querySelector('[aria-label="Edit dataset orders"]'))
    await act(async () =>
      Simulate.change(document.querySelector('#semanticMetadata_description'), { target: { value: 'My draft' } })
    )
    defHttp.put.mockRejectedValue({
      response: { data: { message: 'Permission denied: MODIFY_SEMANTIC_MODEL is required' } }
    })
    await click(findText('.ant-modal button', 'Save'))
    expect(document.querySelector('.ant-modal .ant-alert').textContent).toContain(
      'Permission denied: MODIFY_SEMANTIC_MODEL is required'
    )
    expect(document.querySelector('#semanticMetadata_description').value).toBe('My draft')
    expect(document.querySelector('.ant-modal')).not.toBeNull()
  })
  it('protects an edited structured draft even after switching back to unchanged text', async () => {
    await renderDetails(definitionWith(() => 'Original text'))
    await selectTab('Overview')
    await click(findText('button', 'Edit Model AI Context'))
    await selectOption('#semanticMetadata_contextFormat', 'Structured')
    await act(async () =>
      Simulate.change(document.querySelector('#semanticMetadata_instructions'), { target: { value: 'Hidden draft' } })
    )
    await selectOption('#semanticMetadata_contextFormat', 'Text')
    expect(document.querySelector('#semanticMetadata_contextText').value).toBe('Original text')
    await click(findText('.ant-modal button', 'Cancel'))
    expect(document.body.textContent).toContain('Discard unsaved changes?')
    await click(findText('.ant-modal button', 'Keep editing'))
    await selectOption('#semanticMetadata_contextFormat', 'Structured')
    expect(document.querySelector('#semanticMetadata_instructions').value).toBe('Hidden draft')
  })
})

it('preserves precise vendor numbers in both formatted and raw views', async () => {
  const raw = '{"external_id":9007199254740993,"threshold":0.10000000000000000001}'
  await renderDetails({ datasets: [], customExtensions: [{ vendorName: 'precise', data: raw }] })
  await selectTab('Overview')
  await expandContext('Model Vendor Extensions')
  const pre = document.querySelector('section[aria-label="precise"] pre')
  expect(pre.textContent).toContain('9007199254740993')
  expect(pre.textContent).toContain('0.10000000000000000001')
  await click(findText('button', 'Show raw value'))
  expect(pre.textContent).toBe(raw)
})

it.each(['field', 'metric'])(
  'keeps the %s preview dialect local to the row and shares it with View SQL',
  async kind => {
    const definition = definitionWith(() => null)
    const entities = kind === 'field' ? definition.datasets[0].fields : definition.metrics
    entities[0].expression.dialects = [
      { dialect: 'TRINO', expression: 'trino_sql' },
      { dialect: 'ANSI_SQL', expression: 'ansi_sql' }
    ]
    entities.push({ name: 'second', expression: structuredClone(entities[0].expression) })
    const original = structuredClone(definition)
    await renderDetails(definition)
    if (kind === 'field')
      await click(
        document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon')
      )
    else await selectTab('Metrics')
    const name = entities[0].name

    const select = async (selector, label) => {
      await act(async () =>
        Simulate.mouseDown(
          document.querySelector(selector).closest('.ant-select').querySelector('.ant-select-selector')
        )
      )
      await click(
        [
          ...document.querySelectorAll(
            '.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option-content'
          )
        ].find(element => element.textContent === label)
      )
    }
    await select(`[aria-label="Preview dialect for ${name}"]`, 'ANSI_SQL')
    const grid = document.querySelector(`[data-refer="semantic-model-${kind === 'field' ? 'fields' : 'metrics'}-grid"]`)
    expect([...grid.querySelectorAll('pre')].map(pre => pre.textContent)).toEqual(['ansi_sql', 'trino_sql'])
    await click(document.querySelector(`[aria-label="View expression for ${name}"]`))
    expect(document.querySelector('.ant-modal pre').textContent).toBe('ansi_sql')
    await select('[aria-label="SQL dialect"]', 'TRINO')
    await click(document.querySelector('.ant-modal-close'))
    expect(grid.querySelector('pre').textContent).toBe('trino_sql')
    expect(definition).toEqual(original)
  }
)

it('expands the complete definition and returns to the same tab', async () => {
  await renderDetails(definitionWith(() => null))
  await selectTab('Definition')
  const definitionText = document.querySelector('[data-refer="semantic-model-definition"]').textContent
  await click(findText('button', 'Expand definition'))
  expect(document.querySelector('.ant-modal pre').textContent).toBe(definitionText)
  expect(findText('.ant-modal button', 'Copy definition')).toBeTruthy()
  await click(document.querySelector('.ant-modal-close'))
  expect(findText('[role="tab"]', 'Definition').getAttribute('aria-selected')).toBe('true')
})

it.each([false, true])(
  'does not refresh or navigate for a save completed after leaving the model (rename=%s)',
  async rename => {
    navigation.name = 'orders'
    const definition = definitionWith(() => null)
    let finish
    defHttp.put.mockImplementation(
      () =>
        new Promise(resolve => {
          finish = resolve
        })
    )
    defHttp.get.mockImplementation(({ url }) =>
      Promise.resolve(
        url.endsWith('/semantic-models')
          ? { identifiers: [] }
          : { semanticModel: { name: navigation.name, definition, properties: {} } }
      )
    )
    await renderDetails(definition)
    await click(document.querySelector('[aria-label="Edit Semantic Model"]'))
    await act(async () =>
      Simulate.change(document.querySelector('[data-refer="semantic-model-comment-field"]'), {
        target: { value: 'Updated' }
      })
    )
    await click(document.querySelector('[data-refer="handle-submit-semantic-model"]'))
    expect(finish).toBeTypeOf('function')
    await act(async () => root.render(null))
    defHttp.get.mockClear()
    await act(async () =>
      finish({ semanticModel: { name: rename ? 'renamed' : navigation.name, definition, properties: {} } })
    )
    expect(navigation.replace).not.toHaveBeenCalled()
    expect(defHttp.get.mock.calls.every(([request]) => request.url.endsWith('/semantic-models'))).toBe(true)
  }
)

it.each(['table', 'other schema'])('never displays same-named %s metadata as the routed model', async source => {
  await renderDetails(definitionWith(() => null))
  const thunk = source === 'table' ? getTableDetails : getSemanticModelDetails

  const args = {
    init: true,
    metalake: 'demo',
    catalog: 'sales',
    schema: source === 'table' ? 'mart' : 'other',
    ...(source === 'table' ? { table: navigation.name } : { semanticModel: navigation.name })
  }
  await act(async () => {
    store.dispatch(thunk.pending('other', args))
    store.dispatch(
      thunk.fulfilled(
        {
          init: true,
          [source === 'table' ? 'table' : 'semanticModel']: {
            name: navigation.name,
            comment: 'Unrelated comment',
            columns: [],
            definition: definitionWith(() => 'unrelated')
          }
        },
        'other',
        args
      )
    )
  })
  expect(document.body.textContent).not.toContain('Unrelated comment')
  expect(document.querySelector('[aria-label="Edit dataset orders"]')).toBeNull()
})

it.each(['field', 'dataset'])('closes the %s snapshot modal when the definition is cleared', async kind => {
  await renderDetails(definitionWith(() => 'guidance'))
  await click(document.querySelector('button.ant-table-row-expand-icon'))
  if (kind === 'dataset') await click(findText('button', 'View dataset details'))
  else {
    await click(document.querySelector('[aria-label="amount AI Context"]'))
  }
  expect(document.querySelector('.ant-modal')).not.toBeNull()
  await act(async () => store.dispatch(resetActivatedDetails()))
  expect(document.querySelector('.ant-modal')).toBeNull()
  expect(defHttp.put).not.toHaveBeenCalled()
})

it.each([
  [undefined, { kind: 'model' }],
  [undefined, { kind: 'field', datasetName: 'orders', name: 'amount' }],
  [{ datasets: [] }, { kind: 'dataset', name: 'orders' }],
  [{ datasets: [{ name: 'orders', fields: [] }] }, { kind: 'field', datasetName: 'orders', name: 'amount' }]
])('renders a closable error when the editor mounts with unavailable metadata', async (definition, target) => {
  const close = vi.fn()
  await act(async () =>
    root.render(
      <Provider store={store}>
        <SemanticMetadataEditor definition={definition} target={target} onClose={close} />
      </Provider>
    )
  )
  expect(document.body.textContent).toContain('The selected metadata no longer exists')
  expect(findText('.ant-modal button', 'Save')).toBeUndefined()
  await click(findText('.ant-modal button', 'Close'))
  expect(close).toHaveBeenCalledOnce()
  expect(defHttp.put).not.toHaveBeenCalled()
})

it.each(['rename', 'remove'])('clears duplicate dialect errors immediately after %s', async correction => {
  const definition = definitionWith(() => null)
  definition.datasets[0].fields[0].expression.dialects = [
    { dialect: 'ANSI_SQL', expression: 'amount' },
    { dialect: 'TRINO', expression: 'amount' }
  ]
  await renderDetails(definition)
  await click(document.querySelector('button.ant-table-row-expand-icon'))
  await click(document.querySelector('[aria-label="Edit field amount"]'))

  const changeDialect = async value =>
    act(async () => {
      Simulate.change(document.querySelector('#semanticMetadata_dialects_1_dialect'), { target: { value } })
      await new Promise(resolve => setTimeout(resolve, 300))
    })
  await changeDialect('ANSI_SQL')
  await click(findText('.ant-modal button', 'Save'))
  expect(document.body.textContent).toContain('Dialect names must be unique')
  if (correction === 'rename') await changeDialect('TRINO')
  else await click(document.querySelector('[aria-label="Remove dialect 2"]'))
  await act(async () => new Promise(resolve => setTimeout(resolve, 400)))
  expect(document.body.textContent).not.toContain('Dialect names must be unique')
  expect(defHttp.put).not.toHaveBeenCalled()
})

it.each(['focused', 'JSON'])(
  'retains the %s draft without overwriting a concurrently edited definition',
  async editor => {
    navigation.name = 'orders'
    const definition = definitionWith(() => null)
    let serverModel = { name: navigation.name, definition, properties: {} }
    defHttp.get.mockImplementation(() => Promise.resolve({ semanticModel: serverModel }))
    await renderDetails(definition)
    let input
    if (editor === 'focused') {
      await click(document.querySelector('[aria-label="Edit dataset orders"]'))
      input = document.querySelector('#semanticMetadata_description')
      await act(async () => Simulate.change(input, { target: { value: 'My draft' } }))
    } else {
      await click(document.querySelector('[aria-label="Edit Semantic Model"]'))
      input = document.querySelector('[data-refer="semantic-model-definition-field"]')
      await act(async () =>
        Simulate.change(input, { target: { value: JSON.stringify({ ...definition, aiContext: 'My draft' }) } })
      )
    }
    serverModel = { ...serverModel, definition: { ...definition, aiContext: 'Concurrent edit' } }
    await click(findText('.ant-modal button', editor === 'focused' ? 'Save' : 'Submit'))
    expect(defHttp.put).not.toHaveBeenCalled()
    expect(document.querySelector('.ant-modal .ant-alert').textContent).toContain('changed since')
    expect(input.value).toContain('My draft')
    expect(store.getState().metalakes.activatedDetails.definition).toEqual(definition)
  }
)

it('edits the time dimension through the field form', async () => {
  let model = { name: navigation.name, definition: definitionWith(() => null), properties: {} }
  model.definition.datasets[0].fields[0].dimension = { isTime: true }
  defHttp.get.mockImplementation(() => Promise.resolve({ semanticModel: model, identifiers: [] }))
  defHttp.put.mockImplementation(({ data }) => {
    model = { ...model, definition: data.updates[0].definition }

    return Promise.resolve({ semanticModel: model })
  })
  await renderDetails(model.definition)
  await click(document.querySelector('button.ant-table-row-expand-icon'))
  await click(document.querySelector('[aria-label="Edit field amount"]'))
  const input = document.querySelector('#semanticMetadata_timeDimension')
  expect(input.closest('.ant-select').textContent).toContain('Yes')
  await act(async () => Simulate.mouseDown(input.closest('.ant-select').querySelector('.ant-select-selector')))
  await click(findText('.ant-select-item-option-content', 'No'))
  await click(findText('.ant-modal button', 'Save'))
  expect(model.definition.datasets[0].fields[0].dimension).toEqual({ isTime: false })
  expect(document.querySelector('.ant-modal')).toBeNull()
})

it.each([true, false])('shows inherited=%s policies without editing or navigation', async inherited => {
  await renderDetails(definitionWith(() => null))
  await selectTab('Overview')
  const policies = [{ name: 'retention', enabled: true, inherited }]
  defHttp.get.mockResolvedValue({ policies })
  await act(async () =>
    store.dispatch(
      getCurrentEntityPolicies({
        init: true,
        metalake: 'demo',
        metadataObjectType: 'semantic_model',
        metadataObjectFullName: `sales.mart.${navigation.name}`,
        details: true
      })
    )
  )
  const link = document.querySelector('[aria-label="View Semantic Model policies"]')
  expect(link.textContent).toBe('1')
  await act(async () => {
    Simulate.mouseEnter(link)
    await new Promise(resolve => setTimeout(resolve, 200))
  })
  expect(document.querySelector('.ant-popover').textContent).toContain('retention')
  expect(document.querySelector('.ant-popover .ant-tag-close-icon')).toBeNull()
  expect(document.querySelector('.ant-popover').textContent).not.toContain('Associate Policy')
  expect(defHttp.get.mock.lastCall[0].url).toContain('/semantic_model/')
  const requestsBeforeClick = defHttp.get.mock.calls.length
  await click(findText('.ant-popover span', 'retention'))
  expect(navigation.push).not.toHaveBeenCalled()
  expect(navigation.replace).not.toHaveBeenCalled()
  expect(defHttp.get).toHaveBeenCalledTimes(requestsBeforeClick)
  expect(link.textContent).toBe('1')
  expect(document.querySelector('.ant-popover').textContent).toContain('retention')
})
