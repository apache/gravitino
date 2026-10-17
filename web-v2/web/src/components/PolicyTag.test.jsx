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
import PolicyTag from './PolicyTag'

const navigation = vi.hoisted(() => ({ push: vi.fn() }))

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), post: vi.fn() } }))
vi.mock('next/navigation', () => ({
  useRouter: () => navigation,
  useSearchParams: () => new URLSearchParams({ metalake: 'demo' })
}))

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
