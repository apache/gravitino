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
import reducer from '@/lib/store/metalakes'
import CatalogsListPage from './page'

const navigation = vi.hoisted(() => ({ params: null, push: vi.fn() }))
vi.mock('next/navigation', () => ({ useSearchParams: () => navigation.params, useRouter: () => navigation }))
vi.mock('./rightContent/RightContent', () => ({ default: () => null }))
vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))

let store
let root
let container
beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('IS_REACT_ACT_ENVIRONMENT', true)
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
  store = configureStore({ reducer: { metalakes: reducer } })
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
})
afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
  vi.unstubAllGlobals()
})

it.each([[[]], [['sales_model', 'other_model']]])(
  'loads all tree siblings on a fresh route with models %j',
  async names => {
    const catalog = { name: 'sales', type: 'relational', provider: 'jdbc-mysql', properties: {}, audit: {} }
    navigation.params = new URLSearchParams({
      metalake: 'demo',
      catalog: 'sales',
      catalogType: 'relational',
      schema: 'mart',
      ...(names.length ? { semanticModel: 'sales_model' } : {})
    })
    defHttp.get.mockImplementation(({ url: requestUrl }) => {
      const url = requestUrl.split('?')[0]

      if (url.endsWith('/catalogs')) return Promise.resolve({ catalogs: [catalog] })
      if (url.endsWith('/schemas')) return Promise.resolve({ identifiers: [{ name: 'mart' }] })
      if (url.endsWith('/schemas/mart')) return Promise.resolve({ schema: { name: 'mart' } })
      if (url.endsWith('/semantic-models/sales_model'))
        return Promise.resolve({ semanticModel: { name: 'sales_model', definition: { datasets: [] } } })
      if (url.endsWith('/tags') || url.endsWith('/policies')) return Promise.resolve({ tags: [], policies: [] })

      const children = url.endsWith('/semantic-models')
        ? names
        : url.endsWith('/tables')
          ? ['orders', 'customers']
          : url.endsWith('/views')
            ? ['recent_orders']
            : ['revenue']

      return Promise.resolve({ identifiers: children.map(name => ({ name })) })
    })
    await act(async () =>
      root.render(
        <Provider store={store}>
          <CatalogsListPage />
        </Provider>
      )
    )

    // Let the existing URL expansion debounce run, then drain its async tree loads.
    await act(async () => new Promise(resolve => setTimeout(resolve, 1100)))
    const schema = store.getState().metalakes.metalakeTree[0].children[0]
    expect(schema.isLeaf).toBe(false)
    expect(schema.children.map(child => child.name).sort()).toEqual(
      ['orders', 'customers', 'recent_orders', 'revenue', ...names].sort()
    )
    for (const name of ['orders', 'customers', 'recent_orders', 'revenue', ...names])
      expect(container.textContent).toContain(name)
    expect(defHttp.get.mock.calls.some(([request]) => request.url.endsWith('/tables'))).toBe(true)
  }
)
