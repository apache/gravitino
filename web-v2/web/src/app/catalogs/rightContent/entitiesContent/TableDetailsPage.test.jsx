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

import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import reducer, { getTableDetails } from '@/lib/store/metalakes'
import { defHttp } from '@/lib/utils/axios'
import TableDetailsPage from './TableDetailsPage'

const namespaces = { metalake: 'demo', catalog: 'sales', schema: 'mart', table: 'orders' }

vi.mock('next/navigation', () => ({
  useSearchParams: () => new URLSearchParams({ ...namespaces, catalogType: 'relational' }),
  useRouter: () => ({ push: vi.fn() })
}))
vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn() } }))
vi.mock('@/components/CustomTags', () => ({ default: () => null }))
vi.mock('@/components/PolicyTag', () => ({ default: () => null }))
vi.mock('../CreateTableDialog', () => ({ default: () => null }))

let container
let root
let store

beforeEach(() => {
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
  vi.stubGlobal(
    'matchMedia',
    vi.fn(() => ({ matches: false, addListener() {}, removeListener() {} }))
  )
  store = configureStore({ reducer: { metalakes: reducer, auth: () => ({ anthEnable: false }) } })
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

async function renderTable(columns) {
  const table = { name: 'orders', columns, distribution: { strategy: 'none', funcArgs: [] } }
  defHttp.get.mockImplementation(({ url }) =>
    Promise.resolve(url.endsWith('/tables/orders') ? { table } : { catalog: { name: 'sales' } })
  )
  await store.dispatch(getTableDetails({ ...namespaces, init: true })).unwrap()
  await act(async () =>
    root.render(
      <Provider store={store}>
        <TableDetailsPage namespaces={namespaces} />
      </Provider>
    )
  )
}

function columnHeaders() {
  return [...container.querySelectorAll('[data-refer="table-grid"] thead th')].map(header => header.textContent.trim())
}

function columnValues(name) {
  const index = columnHeaders().indexOf(name)

  return [...container.querySelectorAll('[data-refer="table-grid"] tbody tr[data-row-key]')].map(
    row => row.cells[index].textContent
  )
}

it('shows table column metadata and lets users hide and restore each column', async () => {
  await renderTable([
    { name: 'id', type: 'long', nullable: false, autoIncrement: true, comment: 'Order ID' },
    {
      name: 'created_at',
      type: 'timestamp',
      nullable: true,
      autoIncrement: false,
      defaultValue: { type: 'function', funcName: 'current_timestamp', funcArgs: [] }
    }
  ])

  const metadata = {
    Nullable: ['No', 'Yes'],
    AutoIncrement: ['Yes', 'No'],
    'Default Value': ['-', 'current_timestamp()'],
    Comment: ['Order ID', '-']
  }
  const headers = ['Column Name', 'Data Type', 'Tags', 'Policies', ...Object.keys(metadata)]
  expect(columnHeaders()).toEqual(headers)

  await act(async () => container.querySelector('button[aria-label="Column settings"]').click())
  for (const [column, values] of Object.entries(metadata)) {
    const checkbox = [...document.querySelectorAll('label')].find(label => label.textContent === column).control
    expect(checkbox.checked).toBe(true)
    expect(columnValues(column)).toEqual(values)

    await act(async () => checkbox.click())
    expect(columnHeaders()).toEqual(headers.filter(header => header !== column))

    await act(async () => checkbox.click())
    expect(columnHeaders()).toEqual(headers)
    expect(columnValues(column)).toEqual(values)
  }
}, 15000)

it('distinguishes empty string and explicit NULL defaults from a missing default', async () => {
  await renderTable([
    { name: 'empty', type: 'string', defaultValue: { type: 'literal', dataType: 'string', value: '' } },
    { name: 'explicit_null', type: 'string', defaultValue: { type: 'literal', dataType: 'null', value: 'NULL' } },
    { name: 'missing', type: 'string' }
  ])

  expect(columnValues('Default Value')).toEqual(["''", 'NULL', '-'])
})
