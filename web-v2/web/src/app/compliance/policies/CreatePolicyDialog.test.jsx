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

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { Simulate } from 'react-dom/test-utils'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import policies from '@/lib/store/policies'
import roles from '@/lib/store/roles'
import CreatePolicyDialog from './CreatePolicyDialog'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn(), post: vi.fn(), put: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))

const policy = {
  name: 'read_sales',
  comment: 'Sales access',
  enabled: true,
  policyType: 'system_access_control',
  content: { privileges: ['select_table'], applicableRoles: ['analyst'] }
}
let root
let container
let close

beforeEach(() => {
  vi.resetAllMocks()
  vi.stubGlobal('IS_REACT_ACT_ENVIRONMENT', true)
  const getComputedStyle = window.getComputedStyle

  // jsdom has no pseudo-element layout; Ant Design uses this to measure scrollbars.
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
  defHttp.get.mockImplementation(({ url }) => {
    if (url.endsWith('/read_sales')) return Promise.resolve({ policy })
    if (url.endsWith('/roles')) return Promise.resolve({ names: ['analyst', 'writer'] })

    return Promise.resolve({ policies: [] })
  })
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
  close = vi.fn()
})

afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

const input = name => document.querySelector(`#policyForm_${name}`)
const button = () => document.querySelector('.ant-modal-footer .ant-btn-primary')

const fill = async (name, value) => {
  await act(async () => Simulate.change(input(name), { target: { value } }))
}

const openSelect = async name => {
  if (input(name).getAttribute('aria-expanded') !== 'true') {
    await act(async () => Simulate.mouseDown(input(name).closest('.ant-select').querySelector('.ant-select-selector')))
  }
}

const select = async (name, label) => {
  await openSelect(name)

  const option = [
    ...document.querySelectorAll('.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option')
  ].find(item => item.textContent === label)
  expect(option).toBeTruthy()
  await act(async () => option.click())
}

const submit = async () => {
  await act(async () => button().click())

  // Ant Design debounces rendering field validation errors.
  await act(async () => new Promise(resolve => setTimeout(resolve, 100)))
}

const renderDialog = async ({ edit = false, anthEnable = true } = {}) => {
  const store = configureStore({ reducer: { policies, roles, auth: () => ({ anthEnable }) } })
  await act(async () =>
    root.render(
      <Provider store={store}>
        <CreatePolicyDialog open setOpen={close} metalake='demo' editPolicy={edit ? 'read_sales' : undefined} />
      </Provider>
    )
  )
}

const fillAccessPolicy = async () => {
  await fill('name', 'read_sales')
  await fill('comment', 'Sales access')
  await select('policyType', 'System Access Control')
  await select('applicableRoles', 'analyst')
  await select('privileges', 'Select Table')
}

describe('access-control policy editor', () => {
  it.each([false, null])('disables access-control creation when authorization is %s', async anthEnable => {
    await renderDialog({ anthEnable })
    await openSelect('policyType')

    const option = [...document.querySelectorAll('.ant-select-item-option')].find(
      item => item.textContent === 'System Access Control'
    )
    expect(option.classList.contains('ant-select-item-option-disabled')).toBe(true)
    await act(async () => option.click())
    expect(input('applicableRoles')).toBeNull()
    expect(defHttp.get).not.toHaveBeenCalled()
  })

  it('does not fetch roles for an existing policy when authorization is disabled', async () => {
    await renderDialog({ edit: true, anthEnable: false })
    expect(input('name').value).toBe('read_sales')
    expect(defHttp.get.mock.calls.map(([request]) => request.url)).toEqual(['/api/metalakes/demo/policies/read_sales'])
  })

  it.each([
    ['applicableRoles', 'privileges', 'Select Table', 'Please select at least one role.'],
    ['privileges', 'applicableRoles', 'analyst', 'Please select at least one privilege.']
  ])('rejects empty %s before sending a request', async (missing, field, label, message) => {
    await renderDialog()
    await fill('name', 'read_sales')
    await select('policyType', 'System Access Control')
    await select(field, label)
    await submit()
    expect(document.body.textContent).toContain(message)
    expect(defHttp.post).not.toHaveBeenCalled()
    expect(close).not.toHaveBeenCalled()
  })

  it('creates an access-control policy with only privileges and applicable roles in its content', async () => {
    defHttp.post.mockResolvedValue({ policy })
    await renderDialog()
    await fillAccessPolicy()
    await submit()
    expect(defHttp.post).toHaveBeenCalledExactlyOnceWith({ url: '/api/metalakes/demo/policies', data: policy })
    expect(close).toHaveBeenCalledWith(false)
  })

  it('edits access-control content with the correct updateContent policy type', async () => {
    defHttp.put.mockResolvedValue({ policy })
    await renderDialog({ edit: true })
    await select('privileges', 'Modify Table')
    await select('applicableRoles', 'writer')
    await submit()
    expect(defHttp.put).toHaveBeenCalledExactlyOnceWith({
      url: '/api/metalakes/demo/policies/read_sales',
      data: {
        updates: [
          {
            '@type': 'updateContent',
            policyType: 'system_access_control',
            newContent: { privileges: ['select_table', 'modify_table'], applicableRoles: ['analyst', 'writer'] }
          }
        ]
      }
    })
    expect(close).toHaveBeenCalledWith(false)
  })

  it.each([false, true])('retains input after a failed save and allows retry (edit=%s)', async edit => {
    const write = edit ? defHttp.put : defHttp.post
    write.mockRejectedValueOnce(new Error('Save failed')).mockResolvedValue({ policy })
    await renderDialog({ edit })
    if (!edit) await fillAccessPolicy()
    await fill('comment', 'Unsaved comment')
    await select('privileges', 'Modify Table')
    await submit()
    expect(write).toHaveBeenCalledTimes(1)
    expect(close).not.toHaveBeenCalled()
    expect(input('name').value).toBe('read_sales')
    expect(input('comment').value).toBe('Unsaved comment')
    expect(input('applicableRoles').closest('.ant-select').textContent).toContain('analyst')
    expect(input('privileges').closest('.ant-select').textContent).toContain('Modify Table')
    expect(button().classList.contains('ant-btn-loading')).toBe(false)
    await submit()
    expect(write).toHaveBeenCalledTimes(2)
    expect(write.mock.calls[1][0]).toEqual(write.mock.calls[0][0])
    expect(close).toHaveBeenCalledWith(false)
  })
})
