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
import reducer, { fetchSemanticModels, setIntoTreeNodeWithFetch, setMetalakeTree, setLoadedNodes } from './index'

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
})
