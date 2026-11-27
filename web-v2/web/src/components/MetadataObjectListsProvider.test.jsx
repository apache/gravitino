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
import { act, StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import metalakes from '@/lib/store/metalakes'
import tags from '@/lib/store/tags'
import Tags from './CustomTags'
import MetadataObjectListsProvider from './MetadataObjectListsProvider'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), post: vi.fn() } }))
vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn() }),
  useSearchParams: () => new URLSearchParams({ metalake: 'demo' })
}))

let root
let container
let store

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('IS_REACT_ACT_ENVIRONMENT', true)
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
  store = configureStore({ reducer: { metalakes, tags } })
  defHttp.get.mockResolvedValue({ tags: [] })
})

afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
  vi.unstubAllGlobals()
})

const endpoint = (metalake = 'demo') => `/api/bulk/metalakes/${metalake}/objects/tags`

const response = (names, type = 'schema') => ({
  code: 0,
  results: names.map(name => ({
    metadataObject: { type, fullName: `catalog.${name}` },
    tags: { code: 0, tags: [{ name: `tag-${name}`, properties: {}, inherited: true }], hasInaccessibleTags: false }
  }))
})
const namesFrom = request => request.data.metadataObjects.map(object => object.fullName.slice('catalog.'.length))

const resolveRequests = (type = 'schema') =>
  defHttp.post.mockImplementation(request => Promise.resolve(response(namesFrom(request), type)))
const settle = async () => act(async () => new Promise(resolve => setTimeout(resolve, 10)))

async function renderRows(names, metalake = 'demo', type = 'schema') {
  await act(async () => {
    root.render(
      <StrictMode>
        <Provider store={store}>
          <MetadataObjectListsProvider key={metalake} metalake={metalake}>
            {names.map(name => (
              <div key={name} data-row={name}>
                <Tags metadataObjectType={type} metadataObjectFullName={`catalog.${name}`} />
              </div>
            ))}
          </MetadataObjectListsProvider>
        </Provider>
      </StrictMode>
    )
  })
  await settle()
}

it('batches tag lookups into one request', async () => {
  let finish
  defHttp.post.mockImplementation(
    () =>
      new Promise(resolve => {
        finish = resolve
      })
  )
  await renderRows(['one', 'two'])
  expect(defHttp.get).not.toHaveBeenCalled()
  expect(defHttp.post).toHaveBeenCalledTimes(1)
  expect(defHttp.post).toHaveBeenCalledWith({
    url: endpoint(),
    data: {
      metadataObjects: [
        { type: 'SCHEMA', fullName: 'catalog.one' },
        { type: 'SCHEMA', fullName: 'catalog.two' }
      ]
    }
  })
  await act(async () => finish(response(['one', 'two'])))
  expect(container.textContent).toContain('tag-one')
  expect(container.textContent).toContain('tag-two')
  expect(container.querySelector('[aria-label="lock"]')).not.toBeNull()
})

it.each(['table', 'TABLE'])('matches responses with type %s', async type => {
  resolveRequests(type)
  await renderRows(['s.customers'], 'demo', 'table')
  expect(container.textContent).toContain('tag-s.customers')
  expect(container.textContent).not.toContain('Unable to load')
})

it('discards responses for rows removed by pagination', async () => {
  let finish
  defHttp.post.mockImplementation(
    () =>
      new Promise(resolve => {
        finish = resolve
      })
  )
  await renderRows(['one'])
  resolveRequests()
  await renderRows(['two'])
  await act(async () => finish(response(['one'])))
  expect(container.textContent).toContain('tag-two')
  expect(container.textContent).not.toContain('tag-one')
  expect(defHttp.post).toHaveBeenCalledTimes(2)
})

it('retries failed tag loads', async () => {
  defHttp.post.mockRejectedValue(new Error('unavailable'))
  await renderRows(['one'])
  expect(container.textContent).toContain('Unable to load tags')
  expect(container.textContent).not.toContain('No Tags')
  const retry = [...container.querySelectorAll('button')].find(button => button.textContent.includes('Retry'))
  expect(retry).toBeDefined()
  resolveRequests()
  await act(async () => retry.click())
  await settle()
  expect(defHttp.post).toHaveBeenCalledTimes(2)
  expect(container.textContent).toContain('tag-one')
})

it('refreshes tags after removing a tag value', async () => {
  let removed = false
  defHttp.post.mockImplementation(request => {
    if (!request.url.includes('/bulk/')) {
      removed = true

      return Promise.resolve({ code: 0 })
    }
    const result = response(namesFrom(request))
    if (removed) result.results[0].tags.tags = []
    else Object.assign(result.results[0].tags.tags[0], { inherited: false, assignmentValues: ['private'] })

    return Promise.resolve(result)
  })
  await renderRows(['one'])
  expect(container.textContent).toContain('tag-one=private')
  const close = container.querySelector('.ant-tag-close-icon')
  expect(close).not.toBeNull()
  await act(async () => close.click())
  await settle()
  expect(container.textContent).toContain('No Tags')
  expect(defHttp.post).toHaveBeenCalledTimes(3)
  expect(defHttp.post.mock.calls[1][0].data).toEqual({ tagsToRemove: [{ name: 'tag-one', value: 'private' }] })
  expect(defHttp.get).not.toHaveBeenCalled()
})

it('does not reuse results across metalakes', async () => {
  resolveRequests()
  await renderRows(['one'])
  let finish
  defHttp.post.mockImplementation(
    () =>
      new Promise(resolve => {
        finish = resolve
      })
  )
  await renderRows(['one'], 'other')
  expect(container.textContent).not.toContain('tag-one')
  expect(defHttp.post).toHaveBeenCalledWith(expect.objectContaining({ url: endpoint('other') }))
  await act(async () => finish(response(['one'])))
  expect(container.textContent).toContain('tag-one')
})

it('bounds requests to 100 objects and renders all chunks together', async () => {
  const names = Array.from({ length: 101 }, (_, index) => `schema${index}`)
  let finishLast
  defHttp.post.mockImplementation(request => {
    if (request.data.metadataObjects.length === 1) {
      return new Promise(resolve => {
        finishLast = resolve
      })
    }

    return Promise.resolve(response(namesFrom(request)))
  })
  await renderRows(names)
  expect(defHttp.post.mock.calls.map(([request]) => request.data.metadataObjects.length)).toEqual([100, 1])
  expect(container.textContent).not.toContain('tag-schema0')
  await act(async () => finishLast(response(names.slice(100))))
  expect(container.textContent).toContain('tag-schema0')
  expect(container.textContent).toContain('tag-schema100')
})

it.each([1008, 1003, 1002])('renders successful rows alongside item error %s without extra requests', async code => {
  defHttp.post.mockImplementation(request => {
    const names = namesFrom(request)

    return Promise.resolve({
      ...response(
        names.filter(name => name !== 's.denied'),
        'table'
      ),
      errors: [
        {
          index: names.indexOf('s.denied'),
          name: 'catalog.s.denied',
          code,
          type: 'RuntimeException',
          message: 'Unavailable'
        }
      ]
    })
  })
  await renderRows(['s.one', 's.denied', 's.two'], 'demo', 'table')

  for (const name of ['s.one', 's.two']) {
    const row = container.querySelector(`[data-row="${name}"]`)
    expect(row.textContent).toContain(`tag-${name}`)
    expect(row.textContent).not.toContain('Unable to load')
  }
  expect(defHttp.post).toHaveBeenCalledTimes(1)
  expect(container.querySelector('[data-row="s.denied"]').textContent).toContain('Unable to load tags')
  expect(defHttp.get).not.toHaveBeenCalled()
})

it.each([1, 2])('loads all rows when the server bulk limit is %s', async limit => {
  defHttp.post.mockImplementation(request =>
    request.data.metadataObjects.length > limit
      ? Promise.reject({ response: { status: 400 } })
      : Promise.resolve(response(namesFrom(request)))
  )
  await renderRows(['one', 'two', 'three', 'four', 'five'])

  for (const name of ['one', 'two', 'three', 'four', 'five']) {
    expect(container.querySelector(`[data-row="${name}"]`).textContent).toContain(`tag-${name}`)
  }
  expect(container.textContent).not.toContain('Unable to load')
})

it.each([
  { label: 'authentication failure', error: { response: { status: 401 } } },
  { label: 'authorization failure', error: { response: { status: 403 } } },
  { label: 'missing endpoint', error: { response: { status: 404 } } },
  { label: 'server error', error: { response: { status: 500 } } },
  { label: 'service outage', error: { response: { status: 503 } } },
  { label: 'network timeout', error: { code: 'ECONNABORTED' } }
])('does not split requests on a $label', async ({ error }) => {
  defHttp.post.mockRejectedValue(error)
  await renderRows(['one', 'two'])

  expect(defHttp.post).toHaveBeenCalledTimes(1)
  for (const name of ['one', 'two']) {
    expect(container.querySelector(`[data-row="${name}"]`).textContent).toContain('Unable to load tags')
  }
})
