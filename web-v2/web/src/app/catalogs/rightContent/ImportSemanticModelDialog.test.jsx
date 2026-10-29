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
import { message } from 'antd'
import { createRoot } from 'react-dom/client'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import reducer, { setLoadedNodes, setMetalakeTree } from '@/lib/store/metalakes'
import ImportSemanticModelDialog from './ImportSemanticModelDialog'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), post: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))
const originalScrollIntoView = Element.prototype.scrollIntoView
const schemaKey = '{{demo}}{{sales}}{{relational}}{{mart}}'
let root
let container
let close
let store

beforeEach(() => {
  vi.clearAllMocks()
  Element.prototype.scrollIntoView = vi.fn()
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
  defHttp.get.mockResolvedValue({ identifiers: [{ name: 'imported', namespace: ['demo', 'sales', 'mart'] }] })
  store = configureStore({ reducer: { metalakes: reducer } })
  store.dispatch(
    setMetalakeTree([{ key: schemaKey, node: 'schema', children: [{ key: 'table', node: 'table', name: 'orders' }] }])
  )
  store.dispatch(setLoadedNodes([schemaKey]))
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
  close = vi.fn()
})

afterEach(async () => {
  await act(async () => {
    root.unmount()
    message.destroy()
  })
  container.remove()
  Element.prototype.scrollIntoView = originalScrollIntoView
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

const renderDialog = async () => {
  await act(async () =>
    root.render(
      <Provider store={store}>
        <ImportSemanticModelDialog open setOpen={close} metalake='demo' catalog='sales' schema='mart' />
      </Provider>
    )
  )
}

const selectFile = async (name, text) => {
  const input = document.querySelector('input[type="file"]')
  const file = new File([text], name)
  file.text = async () => text
  Object.defineProperty(input, 'files', { configurable: true, value: [file] })
  await act(async () => input.dispatchEvent(new Event('change', { bubbles: true })))
}

const submit = async () => {
  await act(async () => {
    document.querySelector('[data-refer="import-semantic-model-submit"]').click()
    await new Promise(resolve => setTimeout(resolve, 50))
  })
}

describe('OSSIE import', () => {
  it.each([
    ['model.yaml', 'application/yaml', 'name: imported\nversion: 0.2.0.dev0\n'],
    ['model.YML', 'application/yaml', 'name: imported\nversion: 0.2.0.dev0\n'],
    ['model.json', 'application/json', '{ "version": "0.2.0.dev0", "name": "imported", "ai_context": "Guide" }']
  ])('imports %s unchanged and refreshes the list and loaded tree', async (name, type, text) => {
    defHttp.post.mockResolvedValue({ semanticModel: { name: 'imported' } })
    await renderDialog()
    expect(document.querySelector('[data-refer="import-semantic-model-submit"]').disabled).toBe(true)
    expect(document.body.textContent).toContain('demo / sales / mart')
    await selectFile(name, text)
    expect(document.body.textContent).toContain('Replace file')
    expect(document.body.textContent).toContain('Remove file')
    expect(document.body.textContent).toContain('bytes')
    expect(document.body.textContent).toContain('Import preview')
    expect(document.body.textContent).toContain('OSSIE version')
    expect(document.body.textContent).toContain('0.2.0.dev0')
    expect(document.body.textContent).toContain('demo / sales / mart')
    expect(document.body.textContent).toContain('imported')
    await submit()
    expect(defHttp.post).toHaveBeenCalledWith(
      expect.objectContaining({
        url: '/api/metalakes/demo/catalogs/sales/schemas/mart/semantic-models/ossie',
        data: text,
        headers: { 'Content-Type': type }
      }),
      { errorMessageMode: 'none' }
    )
    expect(defHttp.post.mock.calls[0][0].transformRequest[0](text)).toBe(text)
    expect(store.getState().metalakes.semanticModels.map(model => model.name)).toEqual(['imported'])
    expect(store.getState().metalakes.metalakeTree[0].children.map(node => node.name)).toEqual(['orders', 'imported'])
    expect(close).toHaveBeenCalledWith(false)
    const link = document.querySelector('.ant-message a')
    expect(link.textContent).toBe('View model')
    expect(new URL(link.href).searchParams.get('semanticModel')).toBe('imported')
  })

  it('shows server validation errors, retains the file, and allows retry', async () => {
    defHttp.post
      .mockRejectedValueOnce({ response: { data: { message: 'datasets[0].source is invalid' } } })
      .mockResolvedValue({ semanticModel: { name: 'imported' } })
    await renderDialog()
    await selectFile('model.json', '{"name":"imported","version":"0.2.0"}')
    await submit()
    expect(document.querySelector('.ant-alert-message').textContent).toBe('Import failed')
    expect(Element.prototype.scrollIntoView).toHaveBeenCalledWith({ block: 'nearest' })
    expect(document.querySelector('.ant-alert-description').textContent).toBe('datasets[0].source is invalid')
    expect(document.body.textContent).toContain('model.json')
    expect(close).not.toHaveBeenCalled()
    expect(defHttp.get).not.toHaveBeenCalled()
    await submit()
    expect(defHttp.post).toHaveBeenCalledTimes(2)
    expect(close).toHaveBeenCalledWith(false)
  })

  it('rejects malformed JSON at preview time without submitting it', async () => {
    await renderDialog()
    await selectFile('model.json', '{ invalid }')
    expect(document.querySelector('.ant-alert-message').textContent).toBe('File preview failed')
    expect(document.body.textContent).toContain('No import was attempted')
    expect(document.querySelector('[data-refer="import-semantic-model-submit"]').disabled).toBe(true)
    expect(defHttp.post).not.toHaveBeenCalled()
  })

  it('ignores a previous file read that finishes after the next selection', async () => {
    await renderDialog()
    let finish

    const text = new Promise(resolve => {
      finish = resolve
    })
    const file = new File([''], 'old.yaml')
    file.text = () => text
    const input = document.querySelector('input[type="file"]')
    Object.defineProperty(input, 'files', { configurable: true, value: [file] })
    await act(async () => input.dispatchEvent(new Event('change', { bubbles: true })))
    await selectFile('new.json', '{"name":"new","version":"0.2.0"}')
    await act(async () => finish('name: old\nversion: 0.2.0'))
    expect(document.querySelector('[data-refer="import-preview"]').textContent).toContain('new')
    expect(document.querySelector('[data-refer="import-preview"]').textContent).not.toContain('old')
  })

  it.each([
    ['model.txt', 'name: invalid', 'Select a .yaml, .yml, or .json file.'],
    ['model.yaml', '  ', 'The selected file is empty.']
  ])('rejects invalid selection %s', async (name, text, error) => {
    await renderDialog()
    await selectFile(name, text)
    await submit()
    expect(document.querySelector('.ant-alert-description').textContent).toBe(error)
    expect(defHttp.post).not.toHaveBeenCalled()
    expect(close).not.toHaveBeenCalled()
  })
})

it.each(['{"name":"model"}', '{"name":"model","version":2}', 'name: model\nversion: " "'])(
  'blocks version-invalid preview %s',
  async text => {
    await renderDialog()
    await selectFile(text.startsWith('{') ? 'model.json' : 'model.yaml', text)
    expect(document.body.textContent).toContain('non-empty string version')
    expect(document.querySelector('[data-refer="import-semantic-model-submit"]').disabled).toBe(true)
    expect(defHttp.post).not.toHaveBeenCalled()
  }
)
