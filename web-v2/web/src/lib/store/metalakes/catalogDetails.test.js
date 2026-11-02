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

import { beforeEach, expect, it, vi } from 'vitest'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import { catalogEntityKey } from '@/lib/utils/catalogEntity'
import reducer, {
  getTableDetails,
  getFilesetDetails,
  getTopicDetails,
  getModelDetails,
  setActiveCatalogEntity,
  updateModel
} from './index'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), put: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))
const namespace = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
let store
beforeEach(() => {
  vi.resetAllMocks()
  store = configureStore({ reducer: { metalakes: reducer } })
})

it.each([undefined, '0'])('refreshes a saved parent model comment and properties for version %s', async version => {
  let model = { name: 'forecast', comment: 'Original', properties: { framework: 'pytorch' } }
  const args = { ...namespace, model: 'forecast', version, init: true }
  store.dispatch(setActiveCatalogEntity(args))
  defHttp.get.mockImplementation(() => Promise.resolve({ model }))
  await store.dispatch(getModelDetails(args))
  defHttp.put.mockImplementation(() => {
    model = { ...model, comment: 'Updated', properties: { framework: 'tensorflow' } }

    return Promise.resolve({ model })
  })
  await store.dispatch(
    updateModel({
      ...args,
      data: {
        updates: [
          { '@type': 'updateComment', newComment: 'Updated' },
          { '@type': 'setProperty', property: 'framework', value: 'tensorflow' }
        ]
      }
    })
  )
  expect(store.getState().metalakes.activatedDetails).toEqual(model)
  expect(store.getState().metalakes.activatedDetailsKey).toBe(catalogEntityKey(args))
})

it.each([
  { model: 'other', version: '0' },
  { model: 'forecast', schema: 'other', version: '0' }
])('does not overwrite another active model after a delayed version-route save: %j', async destination => {
  const args = { ...namespace, model: 'forecast', version: '0', init: true }
  store.dispatch(setActiveCatalogEntity(args))
  let finish
  defHttp.put.mockImplementation(
    () =>
      new Promise(resolve => {
        finish = resolve
      })
  )
  const save = store.dispatch(updateModel({ ...args, data: { updates: [] } }))
  const next = { ...args, ...destination }
  store.dispatch(setActiveCatalogEntity(next))
  const current = { name: next.model, comment: 'Current model', properties: { keep: 'yes' } }
  defHttp.get
    .mockResolvedValueOnce({ model: current })
    .mockResolvedValue({ model: { name: 'forecast', comment: 'Old save' } })
  await store.dispatch(getModelDetails(next))
  finish({ model: { name: 'forecast' } })
  await save
  expect(store.getState().metalakes.activatedDetails).toEqual(current)
  expect(store.getState().metalakes.activatedDetailsKey).toBe(catalogEntityKey(next))
})

it.each([
  [getTableDetails, 'table', 'relational'],
  [getFilesetDetails, 'fileset', 'fileset'],
  [getTopicDetails, 'topic', 'messaging']
])('populates active $typePrefix details and finishes loading', async (fetch, kind, catalogType) => {
  const args = { ...namespace, [kind]: 'orders', init: true }
  const details = { name: 'orders', comment: 'Active entity', properties: { retained: 'yes' } }
  if (kind === 'table')
    Object.assign(details, {
      columns: [{ name: 'id', type: 'long' }],
      partitioning: [{ strategy: 'identity', fieldName: ['id'] }],
      distribution: { strategy: 'hash', number: 4, funcArgs: [{ fieldName: ['id'] }] }
    })
  store.dispatch(setActiveCatalogEntity(args))
  let respond
  defHttp.get.mockImplementation(
    () =>
      new Promise(resolve => {
        respond = resolve
      })
  )
  const request = store.dispatch(fetch(args))
  expect(store.getState().metalakes.tableLoading).toBe(true)
  respond({ [kind]: details })
  await request
  const state = store.getState().metalakes
  expect(state.activatedDetails.comment).toBe('Active entity')
  expect(state.activatedDetails.properties).toEqual({ retained: 'yes' })
  expect(state.activatedDetailsKey).toBe(catalogEntityKey(args))
  expect(state.tableLoading).toBe(false)
  expect(state.expandedNodes).toContain(`{{demo}}{{sales}}{{${catalogType}}}{{mart}}`)
  if (kind === 'table') {
    expect(state.tableData).toEqual([{ name: 'id', type: 'long', node: 'tableColumn' }])
    expect(state.tableProps.find(prop => prop.type === 'partitioning').items[0].text).toBe('identity(id)')
    expect(state.tableProps.find(prop => prop.type === 'distribution').items[0].text).toBe('hash[4](id)')
  }
})
