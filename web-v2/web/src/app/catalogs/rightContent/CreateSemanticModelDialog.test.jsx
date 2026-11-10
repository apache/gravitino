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
import { Simulate } from 'react-dom/test-utils'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import reducer from '@/lib/store/metalakes'
import CreateSemanticModelDialog from './CreateSemanticModelDialog'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), post: vi.fn(), put: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))

const definition = { datasets: [{ name: 'orders', source: { namespace: ['sales', 'mart'], name: 'orders' } }] }

const model = {
  name: 'orders',
  comment: 'original',
  definition,
  properties: { name: 'literal', comment: '', definition: 'JSON' }
}
let root
let container
let close

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
  defHttp.get.mockImplementation(({ url }) =>
    Promise.resolve(url.endsWith('/orders') ? { semanticModel: model } : { identifiers: [] })
  )
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
  close = vi.fn()
})

afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

const input = name => document.querySelector(`[data-refer="semantic-model-${name}-field"]`)
const button = () => document.querySelector('[data-refer="handle-submit-semantic-model"]')

const fill = async (name, value) => {
  await act(async () => Simulate.change(input(name), { target: { value } }))
}

const submit = async () => {
  await act(async () => {
    button().click()
    await new Promise(resolve => setTimeout(resolve, 50))
  })
}

const renderDialog = async edit => {
  const store = configureStore({ reducer: { metalakes: reducer, auth: () => ({ systemConfig: {} }) } })
  await act(async () =>
    root.render(
      <Provider store={store}>
        <CreateSemanticModelDialog
          open
          setOpen={close}
          metalake='demo'
          catalog='sales'
          schema='mart'
          editSemanticModel={edit ? 'orders' : undefined}
        />
      </Provider>
    )
  )
}

describe('semantic model editor', () => {
  it.each([false, true])('allows the model name ossie (edit=%s)', async edit => {
    defHttp.post.mockResolvedValue({ semanticModel: { ...model, name: 'ossie' } })
    defHttp.put.mockResolvedValue({ semanticModel: { ...model, name: 'ossie' } })
    await renderDialog(edit)
    await fill('name', 'ossie')
    if (!edit) await fill('definition', JSON.stringify(definition))
    await submit()
    expect(edit ? defHttp.put : defHttp.post).toHaveBeenCalledOnce()
    expect(close).toHaveBeenCalledWith(false)
  })

  it('blocks a nested expression error before submission', async () => {
    await renderDialog(false)
    await fill('name', 'orders')
    await fill(
      'definition',
      JSON.stringify({ datasets: [{ ...definition.datasets[0], fields: [{ name: 'amount' }] }] })
    )
    await submit()
    expect(document.body.textContent).toContain('datasets[0].fields[0].expression must have non-empty dialects.')
    expect(defHttp.post).not.toHaveBeenCalled()
    expect(close).not.toHaveBeenCalled()
  })

  it.each([false, true])('retains input on failed save and supports retry (edit=%s)', async edit => {
    const write = edit ? defHttp.put : defHttp.post
    write.mockRejectedValueOnce(new Error('Invalid dataset source')).mockResolvedValue({ semanticModel: model })
    await renderDialog(edit)
    if (!edit) {
      await fill('name', 'orders')
      await fill('definition', JSON.stringify(definition))
    }
    await fill('comment', 'unsaved comment')
    const text = input('definition').value
    await submit()
    expect(write).toHaveBeenCalledTimes(1)
    expect(close).not.toHaveBeenCalled()
    expect(input('comment').value).toBe('unsaved comment')
    expect(input('definition').value).toBe(text)
    expect(button().classList.contains('ant-btn-loading')).toBe(false)
    await submit()
    expect(write).toHaveBeenCalledTimes(2)
    expect(close).toHaveBeenCalledWith(false)
  })

  it('does not replace properties named after form fields', async () => {
    defHttp.put.mockResolvedValue({ semanticModel: model })
    await renderDialog(true)
    await fill('comment', 'new comment')
    await submit()
    expect(defHttp.put.mock.calls[0][0].data).toEqual({
      updates: [{ '@type': 'updateComment', newComment: 'new comment' }]
    })
  })

  it.each(['constructor', 'toString'])('removes a property named %s through the editor', async name => {
    defHttp.get.mockResolvedValueOnce({ semanticModel: { ...model, properties: { [name]: 'value' } } })
    defHttp.put.mockResolvedValue({ semanticModel: model })
    await renderDialog(true)
    await act(async () =>
      document.querySelector('.ant-modal .lucide-minus').dispatchEvent(new MouseEvent('click', { bubbles: true }))
    )
    await submit()
    expect(defHttp.put.mock.calls[0]?.[0].data).toEqual({
      updates: [{ '@type': 'removeProperty', property: name }]
    })
    expect(close).toHaveBeenCalledWith(false)
  })

  it('disables saving when the model could not be loaded', async () => {
    defHttp.get.mockRejectedValue(new Error('Permission denied'))
    await renderDialog(true)
    expect(button().disabled).toBe(true)
    expect(close).not.toHaveBeenCalled()
    expect(defHttp.put).not.toHaveBeenCalled()
  })

  it('closes an unchanged edit without a write request', async () => {
    await renderDialog(true)
    await submit()
    expect(defHttp.put).not.toHaveBeenCalled()
    expect(close).toHaveBeenCalledWith(false)
  })

  it('renders each validation problem separately without sending a request', async () => {
    await renderDialog(false)
    await fill('name', 'orders')
    await fill('definition', JSON.stringify({ datasets: [{ ...definition.datasets[0], fields: [{}, null] }] }))
    await submit()
    expect([...document.querySelectorAll('.ant-form-item-explain-error li')].map(item => item.textContent)).toEqual([
      'datasets[0].fields[0] must have a non-empty name.',
      'datasets[0].fields[1] must have a non-empty name.'
    ])
    expect(defHttp.post).not.toHaveBeenCalled()
    expect(close).not.toHaveBeenCalled()
  })
})
