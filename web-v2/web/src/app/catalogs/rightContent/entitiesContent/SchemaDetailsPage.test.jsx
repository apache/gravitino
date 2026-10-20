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

import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import reducer, { fetchSemanticModels, setActiveCatalogEntity } from '@/lib/store/metalakes'
import SchemaDetailsPage from './SchemaDetailsPage'

const namespace = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
vi.mock('next/navigation', () => ({
  useSearchParams: () => new URLSearchParams({ ...namespace, catalogType: 'relational' }),
  useRouter: () => ({ push: vi.fn() })
}))
vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))
let store
let root
let container
beforeEach(() => {
  vi.resetAllMocks()
  vi.stubGlobal('IS_REACT_ACT_ENVIRONMENT', true)
  const getComputedStyle = window.getComputedStyle
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
  store = configureStore({ reducer: { metalakes: reducer, auth: () => ({ anthEnable: false, systemConfig: {} }) } })
  store.dispatch(setActiveCatalogEntity(namespace))
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

it.each(['success', 'failure', 'abort'])(
  'shows the schema model-list spinner only while its list is pending: %s',
  async outcome => {
    let finish
    let fail
    defHttp.get.mockImplementation(({ url }) => {
      if (url.endsWith('/semantic-models'))
        return new Promise((resolve, reject) => {
          finish = resolve
          fail = reject
        })

      return Promise.resolve({ catalog: { name: 'sales', provider: 'jdbc-mysql' } })
    })
    await act(async () =>
      root.render(
        <Provider store={store}>
          <SchemaDetailsPage />
        </Provider>
      )
    )
    await act(async () =>
      [...document.querySelectorAll('[role="tab"]')].find(tab => tab.textContent === 'Semantic Models').click()
    )
    const spinning = () => container.querySelectorAll('.ant-spin-spinning').length

    const waitForSpinner = expected =>
      vi.waitFor(async () => {
        // Ant Design debounces spinner transitions, including when delay is zero.
        await act(async () => new Promise(resolve => setTimeout(resolve, 10)))
        expect(spinning()).toBe(expected)
      })
    expect(spinning()).toBe(0)
    let request
    await act(async () => {
      request = store.dispatch(fetchSemanticModels({ ...namespace, init: true }))
    })
    await waitForSpinner(1)
    await act(async () => {
      if (outcome === 'success') finish({ identifiers: [{ name: 'orders' }] })
      else if (outcome === 'failure') fail(new Error('Server unavailable'))
      else request.abort()
      await request
    })
    await waitForSpinner(0)
    if (outcome === 'success')
      expect(container.querySelector('[data-refer="semantic-model-list-grid"]').textContent).toContain('orders')
    let background
    await act(async () => {
      background = store.dispatch(fetchSemanticModels({ ...namespace, schema: 'other', init: true }))
    })
    expect(spinning()).toBe(0)
    await act(async () => {
      finish({ identifiers: [] })
      await background
    })
  }
)
