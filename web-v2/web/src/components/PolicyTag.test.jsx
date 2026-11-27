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
  expect(container.textContent.includes('Associate Policy')).toBe(false)

  const close = directTag.querySelector('.ant-tag-close-icon')
  expect(close).toBeNull()

  const policyLink = [...container.querySelectorAll('.ant-tag')].find(tag => tag.textContent === 'inherited')
  await act(async () => policyLink.click())
  expect(navigation.push).toHaveBeenCalledExactlyOnceWith('/metadataObjectsForPolicy?policy=inherited&metalake=demo')
  if (readOnly) expect(defHttp.post).not.toHaveBeenCalled()
})

it.each(['table', 'schema', 'catalog'])('keeps policy associations read-only on %s', async metadataObjectType => {
  defHttp.get.mockResolvedValue({ policies: [{ name: 'access', enabled: true, policyType: 'system_access_control' }] })
  const store = configureStore({ reducer: { metalakes, policies } })
  await act(async () =>
    root.render(
      <Provider store={store}>
        <PolicyTag metadataObjectType={metadataObjectType} metadataObjectFullName='sales.mart.orders' />
      </Provider>
    )
  )

  expect(container.textContent).toContain('access')
  expect(container.textContent).not.toContain('Associate Policy')
  expect(container.querySelector('.ant-tag-close-icon')).toBeNull()
  expect(defHttp.post).not.toHaveBeenCalled()
  expect(defHttp.delete).not.toHaveBeenCalled()
})

const renderTagPolicies = async (
  options = [{ name: 'masking', enabled: true }],
  allowedValues = [],
  onBindingsChange
) => {
  const store = configureStore({
    reducer: { metalakes, policies },
    preloadedState: {
      policies: { policiesData: options, policiesLoading: false, error: null }
    }
  })
  await act(async () =>
    root.render(
      <Provider store={store}>
        <PolicyTag
          metadataObjectType='tag'
          metadataObjectFullName='classification.sensitive'
          allowedValues={allowedValues}
          onBindingsChange={onBindingsChange}
        />
      </Provider>
    )
  )
}

const selectPolicy = async name => {
  const option = [...document.body.querySelectorAll('.ant-cascader-menu-item')].find(item => item.textContent === name)
  await act(async () => option.click())
}

it.each(['Finance', 'all', 'custom'])('adds and displays the exact allowed value %s', async value => {
  const selector = { type: 'TAG_VALUE', value }
  const onBindingsChange = vi.fn()
  defHttp.get.mockResolvedValueOnce({ associations: [] }).mockResolvedValueOnce({
    associations: [{ policy: { name: 'masking', enabled: true }, selector }]
  })
  defHttp.post.mockResolvedValue({})

  await renderTagPolicies(undefined, [value, 'Sales'], onBindingsChange)
  await act(async () =>
    [...container.querySelectorAll('.ant-tag')].find(tag => tag.textContent === 'Associate Policy').click()
  )
  await selectPolicy('masking')
  expect(defHttp.post).not.toHaveBeenCalled()
  await selectPolicy(value)

  expect(defHttp.post).toHaveBeenCalledExactlyOnceWith({ url: `${tagPoliciesUrl}/masking`, data: { selector } })
  expect(container.textContent).toContain(`masking · ${value}`)
  expect(onBindingsChange).toHaveBeenCalledExactlyOnceWith('classification.sensitive')
})

it('offers an all-values mapping for a tag with allowed values', async () => {
  defHttp.get.mockResolvedValue({ associations: [] })
  defHttp.post.mockResolvedValue({})

  await renderTagPolicies(undefined, ['Finance'])
  await act(async () => container.querySelector('.ant-tag').click())
  await selectPolicy('masking')
  await selectPolicy('All values')

  expect(defHttp.post).toHaveBeenCalledExactlyOnceWith({
    url: `${tagPoliciesUrl}/masking`,
    data: { selector: { type: 'ALL_VALUES' } }
  })
})

it('trims a custom selector for an unrestricted tag', async () => {
  defHttp.get.mockResolvedValue({ associations: [] })
  defHttp.post.mockResolvedValue({})

  await renderTagPolicies(undefined, null)
  await act(async () => container.querySelector('.ant-tag').click())
  await selectPolicy('masking')
  await selectPolicy('Enter a value')
  const input = container.querySelector('input[aria-label="Tag value for masking"]')
  const associate = [...container.querySelectorAll('button')].find(button => button.textContent === 'Associate')
  await act(async () => associate.click())
  expect(defHttp.post).not.toHaveBeenCalled()
  await act(async () => {
    input.value = ' Finance '
    associate.click()
  })

  expect(defHttp.post).toHaveBeenCalledExactlyOnceWith({
    url: `${tagPoliciesUrl}/masking`,
    data: { selector: { type: 'TAG_VALUE', value: 'Finance' } }
  })
})

it('lists policies associated with a tag using the association response mapping', async () => {
  defHttp.get.mockResolvedValue({
    associations: [{ policy: { name: 'masking', enabled: true }, selector: { type: 'ALL_VALUES' } }]
  })

  await renderTagPolicies()

  expect(defHttp.get).toHaveBeenCalledExactlyOnceWith({ url: tagPoliciesUrl, params: { details: true } })
  expect(container.textContent).toContain('masking')
  expect(container.textContent).not.toContain('No Policies')
})

it('binds a no-value tag directly and displays only the policy name', async () => {
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
  expect(container.querySelector('.ant-tag').textContent).toBe('masking')
})

it('removes a tag policy when DELETE returns an empty 204 response', async () => {
  const onBindingsChange = vi.fn()
  defHttp.get
    .mockResolvedValueOnce({
      associations: [{ policy: { name: 'masking', enabled: true }, selector: { type: 'ALL_VALUES' } }]
    })
    .mockResolvedValueOnce({ associations: [] })
  defHttp.delete.mockResolvedValue(undefined)

  await renderTagPolicies(undefined, [], onBindingsChange)
  const close = container.querySelector('.ant-tag-close-icon')
  await act(async () => close.click())

  expect(defHttp.delete).toHaveBeenCalledExactlyOnceWith({ url: `${tagPoliciesUrl}/masking` })
  expect(container.textContent).toContain('No Policies')
  expect(onBindingsChange).toHaveBeenCalledExactlyOnceWith('classification.sensitive')
})

it.each(['add', 'remove'])('notifies the parent when a policy %s finishes after unmount', async operation => {
  const onBindingsChange = vi.fn()
  const adding = operation === 'add'
  let completeMutation

  const request = adding ? defHttp.post : defHttp.delete
  request.mockImplementationOnce(
    () =>
      new Promise(resolve => {
        completeMutation = resolve
      })
  )
  const binding = { policy: { name: 'masking', enabled: true }, selector: { type: 'ALL_VALUES' } }
  defHttp.get.mockResolvedValue({ associations: adding ? [] : [binding] })
  await renderTagPolicies(undefined, [], onBindingsChange)

  if (adding) {
    await act(async () => container.querySelector('.ant-tag').click())
    await selectPolicy('masking')
  } else {
    await act(async () => container.querySelector('.ant-tag-close-icon').click())
  }
  await act(async () => root.render(null))
  await act(async () => completeMutation({}))

  expect(defHttp.get).toHaveBeenCalledTimes(1)
  expect(onBindingsChange).toHaveBeenCalledExactlyOnceWith('classification.sensitive')
})

it('refreshes an existing row after another editor changes its bindings', async () => {
  const store = configureStore({ reducer: { metalakes, policies } })

  const renderRow = async refreshVersion =>
    act(async () =>
      root.render(
        <Provider store={store}>
          <PolicyTag
            metadataObjectType='tag'
            metadataObjectFullName='classification.sensitive'
            allowedValues={['Finance']}
            refreshVersion={refreshVersion}
          />
        </Provider>
      )
    )
  defHttp.get.mockResolvedValueOnce({
    associations: [{ policy: { name: 'masking', enabled: true }, selector: { type: 'ALL_VALUES' } }]
  })
  await renderRow(0)
  expect(container.textContent).toContain('masking · All values')

  defHttp.get.mockResolvedValueOnce({
    associations: [{ policy: { name: 'masking', enabled: true }, selector: { type: 'TAG_VALUE', value: 'Finance' } }]
  })
  await renderRow(1)
  expect(container.textContent).toContain('masking · Finance')
  expect(defHttp.get).toHaveBeenCalledTimes(2)
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

it('offers access-control policies for tag association', async () => {
  defHttp.get.mockResolvedValue({ associations: [] })
  defHttp.post.mockResolvedValue({})
  await renderTagPolicies([{ name: 'access', enabled: true, policyType: 'system_access_control' }])
  await act(async () =>
    [...container.querySelectorAll('.ant-tag')].find(tag => tag.textContent === 'Associate Policy').click()
  )
  await selectPolicy('access')

  expect(defHttp.post).toHaveBeenCalledExactlyOnceWith({
    url: `${tagPoliciesUrl}/access`,
    data: { selector: { type: 'ALL_VALUES' } }
  })
})
