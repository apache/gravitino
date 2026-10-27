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
  resetMetalakeStore,
  deleteSemanticModel,
  fetchSemanticModels,
  fetchTables,
  fetchViews,
  getSemanticModelDetails,
  updateSemanticModel,
  setIntoTreeNodeWithFetch,
  setLoadedNodes,
  setMetalakeTree,
  setActiveCatalogEntity
} from './index'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), delete: vi.fn(), put: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))

const namespace = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
const schemaKey = '{{demo}}{{sales}}{{relational}}{{mart}}'
let store

beforeEach(() => {
  vi.clearAllMocks()
  store = configureStore({ reducer: { metalakes: reducer } })
})

describe('definition replacement conflict checks', () => {
  const original = { datasets: [], aiContext: 'Original guidance' }
  const updated = { ...original, aiContext: 'My draft' }

  const args = {
    ...namespace,
    semanticModel: 'orders',
    expectedDefinition: original,
    data: { updates: [{ '@type': 'replaceDefinition', definition: updated }] }
  }

  it('does not change another page loading state after a delayed verification read', async () => {
    store.dispatch(setActiveCatalogEntity({ ...namespace, semanticModel: 'orders' }))
    let verify
    let finishSave
    defHttp.get.mockImplementation(({ url }) =>
      url.endsWith('/orders')
        ? new Promise(resolve => {
            verify = resolve
          })
        : Promise.resolve({ identifiers: [] })
    )
    defHttp.put.mockImplementation(
      () =>
        new Promise(resolve => {
          finishSave = resolve
        })
    )
    const save = store.dispatch(updateSemanticModel(args))
    store.dispatch(setActiveCatalogEntity({ ...namespace, schema: 'other' }))
    verify({ semanticModel: { definition: original } })
    await vi.waitFor(() => expect(finishSave).toBeTypeOf('function'))
    expect(store.getState().metalakes.tableLoading).toBe(false)
    finishSave({ semanticModel: { name: 'orders', definition: updated } })
    await save
    expect(store.getState().metalakes.tableLoading).toBe(false)
    expect(store.getState().metalakes.semanticModels).toEqual([])
  })

  it('refuses to overwrite a definition changed since the editor opened', async () => {
    defHttp.get.mockResolvedValue({ semanticModel: { definition: { ...original, aiContext: 'Other writer' } } })
    const result = await store.dispatch(updateSemanticModel(args))
    expect(result.payload.err).toBe(true)
    expect(result.payload.message).toContain('changed since')
    expect(defHttp.put).not.toHaveBeenCalled()
    expect(store.getState().metalakes.tableLoading).toBe(false)
  })

  it('compares definitions independently of object key order before saving', async () => {
    defHttp.get
      .mockResolvedValueOnce({ semanticModel: { definition: { aiContext: 'Original guidance', datasets: [] } } })
      .mockResolvedValue({ identifiers: [] })
    defHttp.put.mockResolvedValue({ semanticModel: { name: 'orders', definition: updated } })
    const result = await store.dispatch(updateSemanticModel(args))
    expect(result.payload.definition).toEqual(updated)
    expect(defHttp.put.mock.calls[0][0].data).toEqual(args.data)
    expect(defHttp.get.mock.invocationCallOrder[0]).toBeLessThan(defHttp.put.mock.invocationCallOrder[0])
  })

  it.each(['error', 'missing definition', 'missing snapshot'])(
    'does not save when verification fails: %s',
    async reason => {
      if (reason === 'error') defHttp.get.mockRejectedValue(new Error('Read denied'))
      else
        defHttp.get.mockResolvedValue({
          semanticModel: reason === 'missing definition' ? {} : { definition: original }
        })

      const result = await store.dispatch(
        updateSemanticModel({ ...args, expectedDefinition: reason === 'missing snapshot' ? undefined : original })
      )
      expect(result.payload.err).toBe(true)
      expect(defHttp.put).not.toHaveBeenCalled()
    }
  )
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
    store.dispatch(fetchSemanticModels.pending('old', namespace))
    store.dispatch(fetchSemanticModels.fulfilled({ semanticModels: [{ name: 'existing' }] }, 'old', namespace))
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

describe('superseded Semantic Model lists', () => {
  it('ignores an old failure after a successful refresh', async () => {
    let rejectInitial
    defHttp.get.mockImplementationOnce(
      () =>
        new Promise((resolve, reject) => {
          rejectInitial = reject
        })
    )
    const initial = store.dispatch(fetchSemanticModels(namespace))
    defHttp.get.mockResolvedValueOnce({ identifiers: [{ name: 'new' }] })
    await store.dispatch(fetchSemanticModels(namespace))
    rejectInitial(new Error('Old request failed'))
    await initial
    expect(store.getState().metalakes.semanticModels.map(model => model.name)).toEqual(['new'])
    expect(toast.error).not.toHaveBeenCalled()
  })

  it('does not replace a newly selected schema with an older schema response', async () => {
    let resolveInitial
    defHttp.get.mockImplementationOnce(
      () =>
        new Promise(resolve => {
          resolveInitial = resolve
        })
    )
    const initial = store.dispatch(fetchSemanticModels(namespace))
    defHttp.get.mockResolvedValueOnce({ identifiers: [{ name: 'other_model' }] })
    await store.dispatch(fetchSemanticModels({ ...namespace, schema: 'other' }))
    resolveInitial({ identifiers: [{ name: 'old_model' }] })
    await initial
    expect(store.getState().metalakes.semanticModels.map(model => model.name)).toEqual(['other_model'])
  })

  it('clears request tracking and ignores in-flight results when resetting the store', async () => {
    let resolveInitial
    defHttp.get.mockImplementationOnce(
      () =>
        new Promise(resolve => {
          resolveInitial = resolve
        })
    )
    const initial = store.dispatch(fetchSemanticModels(namespace))
    store.dispatch(resetMetalakeStore())
    resolveInitial({ identifiers: [{ name: 'old_model' }] })
    await initial
    expect(store.getState().metalakes.semanticModels).toEqual([])
    expect(store.getState().metalakes.semanticModelRequests).toEqual({})
    expect(store.getState().metalakes.semanticModelListRequestId).toBeNull()
  })
})

describe('semantic metadata update errors', () => {
  it.each([
    [{ response: { data: { message: 'Permission denied for this model' } } }, 'Permission denied for this model'],
    [{ response: { data: { error: { message: 'Unknown source column amount' } } } }, 'Unknown source column amount'],
    [new Error('Network unavailable'), 'Network unavailable']
  ])('retains the actionable server reason', async (error, message) => {
    defHttp.put.mockRejectedValueOnce(error)

    const result = await store.dispatch(
      updateSemanticModel({ ...namespace, semanticModel: 'orders', data: { updates: [] } })
    )
    expect(result.payload).toEqual({ err: true, message })
    expect(store.getState().metalakes.tableLoading).toBe(false)
    expect(defHttp.get).not.toHaveBeenCalled()
  })
  it('reports an empty update response instead of closing the editor', async () => {
    defHttp.put.mockResolvedValue({})

    const result = await store.dispatch(
      updateSemanticModel({ ...namespace, semanticModel: 'orders', data: { updates: [] } })
    )
    expect(result.payload).toEqual({ err: true, message: 'Failed to update the Semantic Model.' })
    expect(defHttp.get).not.toHaveBeenCalled()
  })
})

describe('Semantic Model lists before full schema tree loading', () => {
  it.each([[[]], [['sales_model', 'other_model']]])('preserves an unloaded schema with models %j', async names => {
    const catalogKey = '{{demo}}{{sales}}{{relational}}'
    const schemaNode = { key: schemaKey, node: 'schema', children: [] }
    store.dispatch(setMetalakeTree([{ key: catalogKey, node: 'catalog', children: [schemaNode] }]))
    defHttp.get.mockImplementation(({ url }) =>
      Promise.resolve({
        identifiers: (url.endsWith('/semantic-models')
          ? names
          : url.endsWith('/tables')
            ? ['orders', 'customers']
            : url.endsWith('/views')
              ? ['recent_orders']
              : ['revenue']
        ).map(name => ({ name }))
      })
    )

    await store.dispatch(fetchSemanticModels({ ...namespace, init: true }))
    const unloaded = store.getState().metalakes.metalakeTree[0].children[0]
    expect(unloaded).toEqual(schemaNode)
    expect(unloaded.isLeaf).toBeUndefined()
    expect(store.getState().metalakes.semanticModels.map(model => model.name)).toEqual(names)

    // TreeComponent marks the node loaded before starting the full children request.
    store.dispatch(setLoadedNodes([schemaKey]))
    await store.dispatch(setIntoTreeNodeWithFetch({ key: schemaKey }))
    const loaded = store.getState().metalakes.metalakeTree[0].children[0]
    expect(loaded.isLeaf).toBe(false)
    expect(loaded.children.map(child => child.name).sort()).toEqual(
      ['orders', 'customers', 'recent_orders', 'revenue', ...names].sort()
    )
    expect(store.getState().metalakes.semanticModelRequests).toEqual({})
  })

  it('keeps a list-only request from modifying an already loaded tree', async () => {
    const schemaNode = { key: schemaKey, node: 'schema', children: [{ name: 'orders', node: 'table' }] }
    store.dispatch(setMetalakeTree([schemaNode]))
    store.dispatch(setLoadedNodes([schemaKey]))
    defHttp.get.mockResolvedValue({ identifiers: [{ name: 'sales_model' }] })
    await store.dispatch(fetchSemanticModels({ ...namespace, init: false }))
    expect(store.getState().metalakes.metalakeTree).toEqual([schemaNode])
    expect(store.getState().metalakes.semanticModels.map(model => model.name)).toEqual(['sales_model'])
  })
})
