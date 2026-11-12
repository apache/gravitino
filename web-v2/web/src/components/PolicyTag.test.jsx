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
import policies, { getPoliciesForTag } from '@/lib/store/policies'
import PolicyTag from './PolicyTag'

const navigation = vi.hoisted(() => ({ push: vi.fn() }))
const tagPoliciesUrl = '/api/metalakes/demo/tags/classification.sensitive/policies'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), post: vi.fn(), delete: vi.fn() } }))
vi.mock('next/navigation', () => ({
  useRouter: () => navigation,
  useSearchParams: () => new URLSearchParams({ metalake: 'demo' })
}))

it('keeps tag selectors nested under their own key', async () => {
  defHttp.get.mockResolvedValue({
    associations: [{ policy: { name: 'masking', enabled: true }, selector: { type: 'ALL_VALUES', name: 'selector' } }]
  })
  const store = configureStore({ reducer: { policies } })

  const result = await store.dispatch(getPoliciesForTag({ metalake: 'demo', tag: 'classification.sensitive' }))

  expect(result.payload).toEqual([
    { name: 'masking', enabled: true, selector: { type: 'ALL_VALUES', name: 'selector' } }
  ])
})

let root
let container

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('IS_REACT_ACT_ENVIRONMENT', true)
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
})

afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
  vi.unstubAllGlobals()
})

it.each([true, false])('respects readOnly=%s while retaining policy navigation', async readOnly => {
  const directPolicy = { name: 'direct', enabled: true, inherited: false }
  const inheritedPolicy = { name: 'inherited', enabled: true, inherited: true }
  defHttp.get.mockResolvedValue({ policies: [directPolicy, inheritedPolicy] })
  const store = configureStore({ reducer: { metalakes, policies } })
  await act(async () =>
    root.render(
      <Provider store={store}>
        <PolicyTag readOnly={readOnly} metadataObjectType='table' metadataObjectFullName='sales.mart.orders' />
      </Provider>
    )
  )
  const [directTag, inheritedTag] = container.querySelectorAll('.ant-tag')
  expect(directTag.textContent).toBe('direct')
  expect(inheritedTag.textContent).toBe('inherited')
  expect(inheritedTag.querySelector('.ant-tag-close-icon')).toBeNull()
  expect(container.textContent.includes('Associate Policy')).toBe(!readOnly)

  const close = directTag.querySelector('.ant-tag-close-icon')
  if (readOnly) {
    expect(close).toBeNull()
  } else {
    expect(close).not.toBeNull()
    defHttp.post.mockResolvedValue({ code: 0 })
    defHttp.get.mockResolvedValue({ policies: [inheritedPolicy] })
    await act(async () => close.click())
    expect(defHttp.post).toHaveBeenCalledExactlyOnceWith({
      url: '/api/metalakes/demo/objects/table/sales.mart.orders/policies',
      data: { policiesToRemove: ['direct'] }
    })
    expect(container.textContent).not.toContain('direct')
    expect(navigation.push).not.toHaveBeenCalled()
  }

  const policyLink = [...container.querySelectorAll('.ant-tag')].find(tag => tag.textContent === 'inherited')
  await act(async () => policyLink.click())
  expect(navigation.push).toHaveBeenCalledExactlyOnceWith('/metadataObjectsForPolicy?policy=inherited&metalake=demo')
  if (readOnly) expect(defHttp.post).not.toHaveBeenCalled()
})

const renderTagPolicies = async () => {
  const store = configureStore({
    reducer: { metalakes, policies },
    preloadedState: {
      policies: { policiesData: [{ name: 'masking', enabled: true }], policiesLoading: false, error: null }
    }
  })
  await act(async () =>
    root.render(
      <Provider store={store}>
        <PolicyTag metadataObjectType='tag' metadataObjectFullName='classification.sensitive' />
      </Provider>
    )
  )
}

const selectPolicy = async name => {
  await act(async () =>
    container.querySelector('.ant-select-selector').dispatchEvent(new MouseEvent('mousedown', { bubbles: true }))
  )
  const option = [...document.body.querySelectorAll('.ant-select-item-option')].find(item => item.textContent === name)
  await act(async () => option.click())
}

it('lists policies associated with a tag using the association response mapping', async () => {
  defHttp.get.mockResolvedValue({
    associations: [{ policy: { name: 'masking', enabled: true }, selector: { type: 'ALL_VALUES' } }]
  })

  await renderTagPolicies()

  expect(defHttp.get).toHaveBeenCalledExactlyOnceWith({ url: tagPoliciesUrl, params: { details: true } })
  expect(container.textContent).toContain('masking')
  expect(container.textContent).not.toContain('No Policies')
})

it('adds a policy to a tag with the ALL_VALUES selector', async () => {
  defHttp.get.mockResolvedValueOnce({ associations: [] }).mockResolvedValueOnce({
    associations: [{ policy: { name: 'masking', enabled: true }, selector: { type: 'ALL_VALUES' } }]
  })
  defHttp.post.mockResolvedValue({})

  await renderTagPolicies()
  await act(async () =>
    [...container.querySelectorAll('.ant-tag')].find(tag => tag.textContent === 'Associate Policy').click()
  )
  await selectPolicy('masking')

  expect(defHttp.post).toHaveBeenCalledExactlyOnceWith({
    url: `${tagPoliciesUrl}/masking`,
    data: { selector: { type: 'ALL_VALUES' } }
  })
  expect(container.textContent).toContain('masking')
})

it('removes a tag policy when DELETE returns an empty 204 response', async () => {
  defHttp.get
    .mockResolvedValueOnce({
      associations: [{ policy: { name: 'masking', enabled: true }, selector: { type: 'ALL_VALUES' } }]
    })
    .mockResolvedValueOnce({ associations: [] })
  defHttp.delete.mockResolvedValue(undefined)

  await renderTagPolicies()
  const close = container.querySelector('.ant-tag-close-icon')
  await act(async () => close.click())

  expect(defHttp.delete).toHaveBeenCalledExactlyOnceWith({ url: `${tagPoliciesUrl}/masking` })
  expect(container.textContent).toContain('No Policies')
})

it('handles a rejected tag policy add request', async () => {
  const association = { policy: { name: 'existing', enabled: true }, selector: { type: 'ALL_VALUES' } }
  defHttp.get.mockResolvedValue({ associations: [association] })
  defHttp.post.mockRejectedValue(new Error('add failed'))

  await renderTagPolicies()
  await act(async () =>
    [...container.querySelectorAll('.ant-tag')].find(tag => tag.textContent === 'Associate Policy').click()
  )
  await selectPolicy('masking')
  expect(defHttp.post).toHaveBeenCalledExactlyOnceWith({
    url: `${tagPoliciesUrl}/masking`,
    data: { selector: { type: 'ALL_VALUES' } }
  })
  expect(container.textContent).toContain('existing')
  expect(container.textContent).not.toContain('No Policies')
})

it('handles a rejected tag policy removal request', async () => {
  const association = { policy: { name: 'masking', enabled: true }, selector: { type: 'ALL_VALUES' } }
  defHttp.get.mockResolvedValue({ associations: [association] })
  defHttp.delete.mockRejectedValue(new Error('remove failed'))

  await renderTagPolicies()

  const close = container.querySelector('.ant-tag-close-icon')
  await act(async () => close.click())
  expect(defHttp.delete).toHaveBeenCalledExactlyOnceWith({ url: `${tagPoliciesUrl}/masking` })
  expect(container.textContent).toContain('masking')
})
