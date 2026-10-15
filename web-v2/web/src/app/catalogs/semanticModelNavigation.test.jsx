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

const navigation = vi.hoisted(() => ({ params: null }))
vi.mock('next/navigation', () => ({ useSearchParams: () => navigation.params }))
vi.mock('./rightContent/RightContent', () => ({ default: () => null }))
vi.mock('./TreeComponent', async () => ({ TreeComponent: (await import('react')).forwardRef(() => null) }))
vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))

let store
let root
let container
let responses
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
  store = configureStore({
    reducer: { metalakes: reducer },
    preloadedState: {
      metalakes: {
        ...reducer(undefined, { type: 'init' }),
        catalogs: [{ name: 'sales', provider: 'jdbc-mysql' }],
        semanticModels: [{ name: 'first' }, { name: 'second' }]
      }
    }
  })
  responses = new Map()
  defHttp.get.mockImplementation(({ url }) => {
    if (/semantic-models\/(first|second)$/.test(url))
      return new Promise(resolve => responses.set(url.split('/').at(-1), resolve))

    return Promise.resolve({ tags: [], policies: [], identifiers: [], schema: { name: 'mart' } })
  })
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
})
afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
  vi.unstubAllGlobals()
})

const navigate = async model => {
  navigation.params = new URLSearchParams({
    metalake: 'demo',
    catalog: 'sales',
    catalogType: 'relational',
    schema: 'mart',
    ...(model ? { semanticModel: model } : {})
  })
  await act(async () =>
    root.render(
      <Provider store={store}>
        <CatalogsListPage />
      </Provider>
    )
  )
}

it.each(['second', null])('ignores old detail follow-ups while navigating to %s', async destination => {
  await navigate('first')
  expect(responses.has('first')).toBe(true)
  await navigate(destination)
  const before = store.getState().metalakes
  const callsBefore = defHttp.get.mock.calls.length
  await act(async () => responses.get('first')({ semanticModel: { name: 'first', definition: { datasets: [] } } }))
  expect(store.getState().metalakes.activatedDetails).toEqual(before.activatedDetails)
  expect(store.getState().metalakes.activatedDetailsLoading).toBe(before.activatedDetailsLoading)
  expect(defHttp.get.mock.calls.length).toBe(callsBefore)
  if (destination) {
    expect(store.getState().metalakes.activatedDetailsLoading).toBe(true)
    await act(async () => responses.get('second')({ semanticModel: { name: 'second', definition: { datasets: [] } } }))
    expect(store.getState().metalakes.activatedDetails.name).toBe('second')
    expect(store.getState().metalakes.activatedDetailsLoading).toBe(false)
  }
})
