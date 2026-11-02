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
import { catalogEntityKey } from '@/lib/utils/catalogEntity'

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
    ...(typeof model === 'object' ? model : model ? { semanticModel: model } : {})
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

it.each(['table', 'schema'])('cancels %s details and all follow-ups when opening a model', async kind => {
  let resolveOld
  defHttp.get.mockImplementation(({ url }) => {
    if (url.endsWith(kind === 'table' ? '/tables/orders' : '/schemas/mart'))
      return new Promise(resolve => {
        resolveOld = resolve
      })

    return Promise.resolve({
      identifiers: [],
      tags: [],
      policies: [],
      semanticModel: { name: 'orders', definition: { datasets: [] } }
    })
  })
  await navigate(kind === 'table' ? { table: 'orders' } : null)
  expect(resolveOld).toBeTypeOf('function')
  await navigate('orders')
  const before = store.getState().metalakes
  const calls = defHttp.get.mock.calls.length
  await act(async () =>
    resolveOld({ table: { name: 'orders', columns: [], distribution: { funcArgs: [] } }, schema: { name: 'mart' } })
  )
  expect(store.getState().metalakes).toEqual(before)
  expect(defHttp.get.mock.calls).toHaveLength(calls)
})

it('ignores a canceled table list and its loading writes after opening a model', async () => {
  let resolveTables
  defHttp.get.mockImplementation(({ url }) => {
    if (url.endsWith('/tables'))
      return new Promise(resolve => {
        resolveTables = resolve
      })

    return Promise.resolve({
      identifiers: [],
      tags: [],
      policies: [],
      schema: { name: 'mart' },
      semanticModel: { name: 'orders', definition: { datasets: [] } }
    })
  })
  await navigate(null)
  expect(resolveTables).toBeTypeOf('function')
  await navigate('orders')
  const before = store.getState().metalakes
  await act(async () => resolveTables({ identifiers: [{ name: 'old_table' }] }))
  expect(store.getState().metalakes).toEqual(before)
})

it.each(['direct link', 'from model'])(
  'loads the parent model header and versions on a version route: %s',
  async entry => {
    const model = { name: 'forecast', comment: 'Model header', properties: { framework: 'pytorch' } }
    defHttp.get.mockResolvedValue({
      model,
      identifiers: [],
      versions: [1, 2],
      tags: [{ name: 'model_tag' }],
      policies: []
    })
    const route = { catalogType: 'model', model: 'forecast' }
    if (entry === 'from model') await navigate(route)
    await navigate({ ...route, version: '1' })
    const state = store.getState().metalakes
    expect(state.activatedDetails).toEqual(model)
    expect(state.activatedDetailsKey).toBe(
      catalogEntityKey({ metalake: 'demo', catalog: 'sales', schema: 'mart', ...route, version: '1' })
    )
    expect(state.activatedDetailsLoading).toBe(false)
    expect(state.tableData.map(item => item.name)).toEqual([1, 2])
    expect(state.currentEntityTags).toEqual([{ name: 'model_tag' }])
    expect(defHttp.get.mock.calls.some(([request]) => request.url.endsWith('/versions/1'))).toBe(false)
  }
)

it('ignores a version route header response after navigating to a Semantic Model', async () => {
  let finishModel
  defHttp.get.mockImplementation(({ url }) => {
    if (url.endsWith('/models/forecast'))
      return new Promise(resolve => {
        finishModel = resolve
      })

    return Promise.resolve({
      identifiers: [],
      versions: [1],
      tags: [],
      policies: [],
      semanticModel: { name: 'orders', definition: { datasets: [] } }
    })
  })
  await navigate({ catalogType: 'model', model: 'forecast', version: '1' })
  expect(finishModel).toBeTypeOf('function')
  await navigate('orders')
  const before = store.getState().metalakes
  const calls = defHttp.get.mock.calls.length
  await act(async () => finishModel({ model: { name: 'forecast', comment: 'Old version route' } }))
  expect(store.getState().metalakes).toEqual(before)
  expect(defHttp.get.mock.calls).toHaveLength(calls)
})

it('loads policies for the active Semantic Model', async () => {
  const policies = [{ name: 'retention', enabled: true, inherited: true }]
  defHttp.get.mockResolvedValue({
    semanticModel: { name: 'orders', definition: { datasets: [] } },
    identifiers: [],
    tags: [],
    policies
  })
  await navigate('orders')
  expect(store.getState().metalakes.currentEntityPolicies).toEqual(policies)
  expect(
    defHttp.get.mock.calls.some(
      ([request]) => request.url.includes('/semantic_model/') && request.url.endsWith('/policies')
    )
  ).toBe(true)
})
