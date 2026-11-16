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
import metalakes from '@/lib/store/metalakes'
import policies from '@/lib/store/policies'
import PolicyAssociationsPage from './page'

const navigation = vi.hoisted(() => ({ push: vi.fn(), back: vi.fn() }))
vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), post: vi.fn() } }))
vi.mock('next/navigation', () => ({
  useRouter: () => navigation,
  useSearchParams: () => new URLSearchParams({ metalake: 'demo', policy: 'retention' })
}))
let root
let container
let associations
beforeEach(() => {
  vi.clearAllMocks()
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
  Object.defineProperty(window, 'matchMedia', {
    configurable: true,
    value: vi.fn(() => ({ matches: false, addListener() {}, removeListener() {} }))
  })
  associations = [
    { tag: { name: 'certified', properties: {}, audit: {} }, selector: { type: 'ALL_VALUES' } },
    { tag: { name: 'data domain', properties: {}, audit: {} }, selector: { type: 'TAG_VALUE', value: 'finance' } }
  ]
  defHttp.get.mockImplementation(({ url }) => {
    if (url.endsWith('/tags')) {
      return associations === null
        ? Promise.reject(new Error('Tag associations unavailable'))
        : Promise.resolve({ code: 0, associations })
    }
    if (url.endsWith('/objects')) {
      return Promise.resolve({ code: 0, metadataObjects: [{ fullName: 'sales.mart.orders', type: 'TABLE' }] })
    }

    return Promise.resolve({ code: 0, policy: { name: 'retention', enabled: true } })
  })
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

const renderPage = async () => {
  const store = configureStore({ reducer: { metalakes, policies } })
  await act(async () =>
    root.render(
      <Provider store={store}>
        <PolicyAssociationsPage />
      </Provider>
    )
  )
}
it('shows associated tags and selectors without direct object mutation controls', async () => {
  await renderPage()
  expect(defHttp.get).toHaveBeenCalledWith({
    url: '/api/metalakes/demo/policies/retention/tags',
    params: { details: true }
  })
  expect(defHttp.get.mock.calls.some(([request]) => request.url.endsWith('/objects'))).toBe(false)
  expect(container.textContent).toContain('certified')
  expect(container.textContent).toContain('All values')
  expect(container.textContent).toContain('finance')
  expect(container.textContent).not.toContain('Actions')
  const tag = [...container.querySelectorAll('button')].find(button => button.textContent === 'data domain')
  expect(tag).toBeDefined()
  await act(async () => tag.click())
  expect(navigation.push).toHaveBeenCalledWith('/metadataObjectsForTag?tag=data%20domain&metalake=demo')
  expect(defHttp.post).not.toHaveBeenCalled()
})
it('shows an empty table when the policy has no associated tags', async () => {
  associations = []
  await renderPage()
  expect(container.textContent).toContain('Associated Tags')
  expect(container.textContent).toContain('No data')
  expect(container.querySelector('[role="alert"]')).toBeNull()
})
it('distinguishes a failed tag lookup from a policy with no associations', async () => {
  associations = null
  await renderPage()
  expect(container.querySelector('[role="alert"]')?.textContent).toContain('Unable to load policy tags.')
  expect(container.textContent).not.toContain('No data')
})
