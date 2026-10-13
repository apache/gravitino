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

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { configureStore } from '@reduxjs/toolkit'
import toast from 'react-hot-toast'
import { defHttp } from '@/lib/utils/axios'
import reducer, {
  deleteSemanticModel,
  fetchSemanticModels,
  fetchTables,
  fetchViews,
  getSemanticModelDetails,
  setIntoTreeNodeWithFetch,
  setLoadedNodes,
  setMetalakeTree
} from './index'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), delete: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))

const namespace = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
const schemaKey = '{{demo}}{{sales}}{{relational}}{{mart}}'
let store

beforeEach(() => {
  vi.clearAllMocks()
  store = configureStore({ reducer: { metalakes: reducer } })
})

describe('semantic model tree identity', () => {
  it('keeps a same-named table and model through expansion and list refreshes', async () => {
    defHttp.get.mockImplementation(({ url }) =>
      Promise.resolve({ identifiers: url.endsWith('/functions') || url.endsWith('/views') ? [] : [{ name: 'orders' }] })
    )
    store.dispatch(setMetalakeTree([{ key: schemaKey, node: 'schema', children: [] }]))
    store.dispatch(setLoadedNodes([schemaKey]))

    const loaded = await store.dispatch(setIntoTreeNodeWithFetch({ key: schemaKey }))
    const children = loaded.payload.data
    const model = children.find(node => node.node === 'semanticModel')
    const table = children.find(node => node.node === 'table')
    expect(model.key).not.toBe(table.key)
    expect(model.id).not.toBe(table.id)

    for (const fetch of [fetchSemanticModels, fetchTables, fetchViews]) {
      await store.dispatch(fetch({ ...namespace, init: true }))
      const refreshed = store.getState().metalakes.metalakeTree[0].children
      expect(
        refreshed
          .filter(node => node.name === 'orders')
          .map(node => node.node)
          .sort()
      ).toEqual(['semanticModel', 'table'])
      expect(refreshed.find(node => node.node === 'semanticModel').key).toBe(model.key)
    }
  })
})

describe('semantic model load errors', () => {
  it.each([true, false])('rejects list failures and retains previous data when init=%s', async init => {
    store.dispatch(fetchSemanticModels.fulfilled({ semanticModels: [{ name: 'existing' }] }, 'old'))
    defHttp.get.mockRejectedValue({ message: 'Server unavailable', response: { status: 500 } })
    const result = await store.dispatch(fetchSemanticModels({ ...namespace, init }))
    expect(result.meta.requestStatus).toBe('rejected')
    expect(result.error.message).toBe('Server unavailable')
    expect(store.getState().metalakes.semanticModels).toEqual([{ name: 'existing' }])
  })

  it.each([fetchSemanticModels, getSemanticModelDetails])(
    'keeps canceled requests quiet for $typePrefix',
    async fetch => {
      defHttp.get.mockRejectedValue(Object.assign(new Error('canceled'), { name: 'CanceledError' }))
      const result = await store.dispatch(fetch({ ...namespace, semanticModel: 'orders' }))
      expect(result.meta.requestStatus).toBe('rejected')
      expect(toast.error).not.toHaveBeenCalled()
    }
  )

  it.each([404, 405])('treats unsupported list endpoints (%s) as empty', async status => {
    defHttp.get.mockRejectedValue({ response: { status } })
    const result = await store.dispatch(fetchSemanticModels(namespace))
    expect(result.payload.semanticModels).toEqual([])
  })

  it.each([fetchSemanticModels, getSemanticModelDetails])(
    'preserves useful error messages for $typePrefix',
    async fetch => {
      defHttp.get.mockRejectedValue({ message: 'Permission denied', response: { status: 403 } })
      const result = await store.dispatch(fetch({ ...namespace, semanticModel: 'orders', init: true }))
      expect(result.meta.requestStatus).toBe('rejected')
      expect(result.error.message).toBe('Permission denied')
    }
  )

  it.each([fetchSemanticModels, getSemanticModelDetails])('reports empty responses for $typePrefix', async fetch => {
    defHttp.get.mockResolvedValue(undefined)
    const result = await store.dispatch(fetch({ ...namespace, semanticModel: 'orders' }))
    expect(result.meta.requestStatus).toBe('rejected')
    expect(result.error.message).toMatch(/empty response/i)
  })
})

describe('semantic model deletion errors', () => {
  it.each([new Error('Permission denied'), { message: 'Permission denied' }])(
    'preserves a deletion error: %j',
    async error => {
      defHttp.delete.mockRejectedValue(error)
      const result = await store.dispatch(deleteSemanticModel({ ...namespace, semanticModel: 'orders' }))
      expect(result.meta.requestStatus).toBe('rejected')
      expect(result.error.message).toBe('Permission denied')
      expect(store.getState().metalakes.tableLoading).toBe(false)
      expect(defHttp.get).not.toHaveBeenCalled()
    }
  )

  it('reports an empty deletion response', async () => {
    defHttp.delete.mockResolvedValue(undefined)
    const result = await store.dispatch(deleteSemanticModel({ ...namespace, semanticModel: 'orders' }))
    expect(result.meta.requestStatus).toBe('rejected')
    expect(result.error.message).toMatch(/empty response/i)
    expect(defHttp.get).not.toHaveBeenCalled()
  })
})
