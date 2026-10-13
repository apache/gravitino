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
import { createRoot } from 'react-dom/client'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import reducer, { getSemanticModelDetails } from '@/lib/store/metalakes'
import SemanticModelDetailsPage from './SemanticModelDetailsPage'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))
vi.mock('next/navigation', () => ({
  useSearchParams: () =>
    new URLSearchParams({ metalake: 'demo', catalog: 'sales', schema: 'mart', semanticModel: 'sales model' })
}))
vi.mock('@/components/GetOwner', () => ({ default: () => null }))
vi.mock('@/components/CustomTags', () => ({ default: () => null }))

let root
let container
let store
let downloads

beforeEach(() => {
  vi.clearAllMocks()
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
      fields: [{ name: 'amount', aiContext: context('field') }]
    }
  ],
  relationships: [{ name: 'customers', from: 'orders', to: 'customers', aiContext: context('relationship') }],
  metrics: [{ name: 'revenue', aiContext: context('metric') }]
})

const renderDetails = async definition => {
  store.dispatch(
    getSemanticModelDetails.fulfilled(
      { semanticModel: { name: 'sales model', definition, properties: {} }, init: true },
      'load'
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
const expandContext = async label => click(findText('.ant-collapse-header', label))
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
    await expandContext('Model AI Context')
    verify('model')
    await click(document.querySelector('[data-refer="semantic-model-datasets-grid"] button.ant-table-row-expand-icon'))
    await expandContext('orders AI Context')
    verify('dataset')
    await click(document.querySelector('[data-refer="semantic-model-fields-grid"] button.ant-table-row-expand-icon'))
    await expandContext('amount AI Context')
    verify('field')
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
