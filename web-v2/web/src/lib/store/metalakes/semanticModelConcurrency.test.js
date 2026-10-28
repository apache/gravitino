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

import { afterEach, expect, it, vi } from 'vitest'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import reducer, {
  fetchSemanticModels,
  setIntoTreeNodeWithFetch,
  setMetalakeTree,
  setLoadedNodes,
  setSelectedNodes,
  getSemanticModelDetails,
  getCurrentEntityTags,
  getCurrentEntityPolicies,
  getSchemaDetails,
  getTableDetails,
  getCatalogDetails,
  getFilesetDetails,
  getTopicDetails,
  getModelDetails,
  getFunctionDetails,
  getViewDetails,
  getMetalakeDetails,
  setActiveCatalogEntity,
  updateSemanticModel
} from './index'

const adapter = defHttp.getAxios().defaults.adapter

afterEach(() => {
  defHttp.getAxios().defaults.adapter = adapter
})

it.each([0, 1])('loads both the schema tab and tree when concurrent response %s finishes first', async first => {
  const namespace = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
  const key = '{{demo}}{{sales}}{{relational}}{{mart}}'
  const store = configureStore({ reducer: { metalakes: reducer } })
  store.dispatch(setMetalakeTree([{ key, node: 'schema', children: [] }]))
  const responses = []

  // Replace only the transport, keeping the real Axios cancellation and response interceptors.
  defHttp.getAxios().defaults.adapter = config =>
    new Promise(resolve => {
      const semanticModels = config.url.split('?')[0].endsWith('/semantic-models')

      const respond = () =>
        resolve({
          config,
          status: 200,
          statusText: 'OK',
          headers: {},
          data: {
            code: 0,
            identifiers: semanticModels ? [{ namespace: ['demo', 'sales', 'mart'], name: 'orders' }] : []
          }
        })
      if (semanticModels) responses.push(respond)
      else respond()
    })

  const list = store.dispatch(fetchSemanticModels({ ...namespace, init: true }))
  await vi.waitFor(() => expect(responses).toHaveLength(1))
  const tree = store.dispatch(setIntoTreeNodeWithFetch({ key }))
  await vi.waitFor(() => expect(responses).toHaveLength(2))
  responses[first]()
  await (first === 0 ? list : tree)
  responses[1 - first]()

  const [listResult, treeResult] = await Promise.all([list, tree])
  expect(listResult.meta.requestStatus).toBe('fulfilled')
  expect(treeResult.meta.requestStatus).toBe('fulfilled')
  expect(store.getState().metalakes.semanticModels.map(model => model.name)).toEqual(['orders'])
  expect(store.getState().metalakes.semanticModelRequests).toEqual({})
  expect(treeResult.payload.data.some(node => node.node === 'semanticModel' && node.name === 'orders')).toBe(true)
})

it.each(['list', 'tree'])('ignores an older %s response after a successful list refresh', async consumer => {
  const namespace = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
  const key = '{{demo}}{{sales}}{{relational}}{{mart}}'
  const store = configureStore({ reducer: { metalakes: reducer } })
  store.dispatch(setMetalakeTree([{ key, node: 'schema', children: [] }]))
  store.dispatch(setLoadedNodes([key]))
  const responses = []
  defHttp.getAxios().defaults.adapter = config =>
    new Promise(resolve => {
      const respond = names =>
        resolve({
          config,
          status: 200,
          statusText: 'OK',
          headers: {},
          data: {
            code: 0,
            identifiers: names.map(name => ({ namespace: ['demo', 'sales', 'mart'], name }))
          }
        })
      if (config.url.split('?')[0].endsWith('/semantic-models')) responses.push(respond)
      else respond([])
    })

  const initial = store.dispatch(
    consumer === 'tree' ? setIntoTreeNodeWithFetch({ key }) : fetchSemanticModels({ ...namespace, init: true })
  )
  await vi.waitFor(() => expect(responses).toHaveLength(1))
  const refresh = store.dispatch(fetchSemanticModels({ ...namespace, init: true }))
  await vi.waitFor(() => expect(responses).toHaveLength(2))
  responses[1](['orders', 'imported'])
  await refresh
  responses[0](['orders'])
  await initial
  expect(store.getState().metalakes.semanticModels.map(model => model.name)).toEqual(['orders', 'imported'])
  expect(store.getState().metalakes.metalakeTree[0].children.map(model => model.name)).toEqual(['orders', 'imported'])
  expect(store.getState().metalakes.semanticModelRequests).toEqual({})
})

it.each(['model', 'schema', 'pending model'])(
  'ignores late model details after navigating to %s',
  async destination => {
    const namespace = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
    const store = configureStore({ reducer: { metalakes: reducer } })
    const responses = []
    defHttp.getAxios().defaults.adapter = config =>
      new Promise(resolve => {
        responses.push(value =>
          resolve({ config, status: 200, statusText: 'OK', headers: {}, data: { code: 0, ...value } })
        )
      })
    const first = store.dispatch(getSemanticModelDetails({ ...namespace, semanticModel: 'first', init: true }))
    await vi.waitFor(() => expect(responses).toHaveLength(1))
    store.dispatch(setSelectedNodes([destination]))

    const next = store.dispatch(
      destination === 'schema'
        ? getSchemaDetails({ ...namespace, init: true })
        : getSemanticModelDetails({ ...namespace, semanticModel: 'second', init: true })
    )
    await vi.waitFor(() => expect(responses).toHaveLength(2))
    if (destination !== 'pending model') {
      responses[1]({ semanticModel: { name: 'second' }, schema: { name: 'mart' } })
      await next
    }
    const before = store.getState().metalakes.activatedDetails
    responses[0]({ semanticModel: { name: 'first' } })
    await first
    expect(store.getState().metalakes.activatedDetails).toEqual(before)
    if (destination === 'pending model') {
      responses[1]({ semanticModel: { name: 'second' } })
      await next
      expect(store.getState().metalakes.activatedDetails.name).toBe('second')
    }
  }
)

it.each([getCurrentEntityTags, getCurrentEntityPolicies])(
  'ignores old entity tags/policies before the next request',
  async fetch => {
    const store = configureStore({ reducer: { metalakes: reducer } })
    let respond
    defHttp.getAxios().defaults.adapter = config =>
      new Promise(resolve => {
        respond = () =>
          resolve({
            config,
            status: 200,
            statusText: 'OK',
            headers: {},
            data: { code: 0, tags: [{ name: 'old' }], policies: [{ name: 'old' }] }
          })
      })

    const request = store.dispatch(
      fetch({
        init: true,
        metalake: 'demo',
        metadataObjectType: 'semantic_model',
        metadataObjectFullName: 'sales.mart.old'
      })
    )
    await vi.waitFor(() => expect(respond).toBeTypeOf('function'))
    store.dispatch(setSelectedNodes(['new']))
    respond()
    await request
    expect(store.getState().metalakes.currentEntityTags).toEqual([])
    expect(store.getState().metalakes.currentEntityPolicies).toEqual([])
  }
)

it.each([
  [getMetalakeDetails, { metalake: 'demo' }, { name: 'demo' }],
  [getCatalogDetails, { metalake: 'demo', catalog: 'sales' }, { catalog: { name: 'sales' } }],
  [getSchemaDetails, {}, { schema: { name: 'mart' } }],
  [getTableDetails, { table: 'orders' }, { table: { name: 'orders', columns: [], distribution: { funcArgs: [] } } }],
  [getFilesetDetails, { fileset: 'orders' }, { fileset: { name: 'orders' } }],
  [getTopicDetails, { topic: 'orders' }, { topic: { name: 'orders' } }],
  [getModelDetails, { model: 'orders' }, { model: { name: 'orders' } }],
  [getFunctionDetails, { functionName: 'orders' }, { function: { name: 'orders' } }],
  [getViewDetails, { view: 'orders' }, { view: { name: 'orders' } }]
])('rejects a late $typePrefix writer after model navigation', async (thunk, args, payload) => {
  const namespace = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
  const store = configureStore({ reducer: { metalakes: reducer } })
  const responses = []
  defHttp.getAxios().defaults.adapter = config =>
    new Promise(resolve => {
      responses.push(data =>
        resolve({ config, status: 200, statusText: 'OK', headers: {}, data: { code: 0, ...data } })
      )
    })
  const old = store.dispatch(thunk({ ...namespace, ...args, init: true }))
  await vi.waitFor(() => expect(responses).toHaveLength(1))
  const nextArgs = { ...namespace, semanticModel: 'orders', init: true }
  store.dispatch(setActiveCatalogEntity(nextArgs))
  const next = store.dispatch(getSemanticModelDetails(nextArgs))
  await vi.waitFor(() => expect(responses).toHaveLength(2))
  responses[1]({ semanticModel: { name: 'orders', definition: { datasets: [] } } })
  await next
  const before = store.getState().metalakes
  responses[0](payload)
  await old
  expect(store.getState().metalakes).toEqual(before)
})

it('refreshes the saved schema tree without replacing the newly selected schema list', async () => {
  const namespace = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
  const key = '{{demo}}{{sales}}{{relational}}{{mart}}'
  const store = configureStore({ reducer: { metalakes: reducer } })
  store.dispatch(setMetalakeTree([{ key, node: 'schema', children: [] }]))
  store.dispatch(setLoadedNodes([key]))
  store.dispatch(setActiveCatalogEntity({ ...namespace, semanticModel: 'orders' }))
  let finishSave
  defHttp.getAxios().defaults.adapter = config =>
    new Promise(resolve => {
      const respond = data =>
        resolve({ config, status: 200, statusText: 'OK', headers: {}, data: { code: 0, ...data } })
      if (config.method === 'put') finishSave = () => respond({ semanticModel: { name: 'renamed' } })
      else respond({ identifiers: [{ name: config.url.includes('/mart/') ? 'renamed' : 'schema_b_model' }] })
    })
  const save = store.dispatch(updateSemanticModel({ ...namespace, semanticModel: 'orders', data: { updates: [] } }))
  await vi.waitFor(() => expect(finishSave).toBeTypeOf('function'))
  store.dispatch(setActiveCatalogEntity({ ...namespace, schema: 'other' }))
  await store.dispatch(fetchSemanticModels({ ...namespace, schema: 'other', init: true }))
  finishSave()
  await save
  expect(store.getState().metalakes.semanticModels.map(model => model.name)).toEqual(['schema_b_model'])
  expect(store.getState().metalakes.metalakeTree[0].children.map(model => model.name)).toEqual(['renamed'])
  expect(store.getState().metalakes.semanticModelRequests).toEqual({})
})

it.each(['success', 'failure', 'abort'])('releases request records after %s across many schemas', async result => {
  const store = configureStore({ reducer: { metalakes: reducer } })
  defHttp.getAxios().defaults.adapter = config => {
    if (result === 'failure') return Promise.reject(new Error('Unavailable'))
    if (result === 'abort') return new Promise(() => {})

    return Promise.resolve({ config, status: 200, statusText: 'OK', headers: {}, data: { code: 0, identifiers: [] } })
  }
  for (let i = 0; i < 20; i++) {
    const request = store.dispatch(fetchSemanticModels({ metalake: 'demo', catalog: 'sales', schema: 'schema_' + i }))
    if (result === 'abort') request.abort()
    await request
  }
  expect(store.getState().metalakes.semanticModelRequests).toEqual({})
})
