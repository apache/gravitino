/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { afterEach, expect, it, vi } from 'vitest'
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import Tags from './CustomTags'

const mocks = vi.hoisted(() => ({
  dispatch: vi.fn(),
  blur: null,
  change: null,
  tagsData: [{ name: 'classification', allowedValues: null }]
}))
vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn() }),
  useSearchParams: () => new URLSearchParams('metalake=demo')
}))
vi.mock('@/lib/hooks/useStore', () => ({
  useAppDispatch: () => mocks.dispatch,
  useAppSelector: () => ({ tagsData: mocks.tagsData })
}))
vi.mock('@/lib/store/tags', () => ({ associateTag: args => ({ kind: 'associate', args }) }))
vi.mock('@/lib/store/metalakes', () => ({ getCurrentEntityTags: args => ({ kind: 'getTags', args }) }))
vi.mock('@ant-design/icons', () => ({ LockOutlined: () => null, PlusOutlined: () => null }))
vi.mock('antd', () => ({
  Cascader: props => {
    mocks.blur = props.onBlur
    mocks.change = props.onChange

    return (
      <div data-testid='cascader'>{props.options.flatMap(option => option.children?.map(child => child.label))}</div>
    )
  },
  Flex: ({ children }) => <div>{children}</div>,
  Input: props => <input {...props} />,
  Space: ({ children }) => <div>{children}</div>,
  Tag: ({ children, onClick }) => <button onClick={onClick}>{children}</button>,
  Tooltip: ({ children }) => <>{children}</>,
  theme: { useToken: () => ({ token: { colorBgContainer: 'white' } }) }
}))

let root
let container
afterEach(async () => {
  if (root) await act(async () => root.unmount())
  container?.remove()
  vi.clearAllMocks()
  mocks.blur = null
  mocks.change = null
  mocks.tagsData = [{ name: 'classification', allowedValues: null }]
})

it('closes the Associate Tag dropdown when dismissed without a selection', async () => {
  mocks.dispatch.mockResolvedValue({ payload: { tags: [] } })
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)

  await act(async () => root.render(<Tags metadataObjectType='table' metadataObjectFullName='sales.orders' />))
  await act(async () =>
    [...container.querySelectorAll('button')].find(button => button.textContent.includes('Associate Tag')).click()
  )
  expect(container.querySelector('[data-testid="cascader"]')).not.toBeNull()

  await act(async () => mocks.blur())

  expect(container.querySelector('[data-testid="cascader"]')).toBeNull()
  expect(container.textContent).toContain('Associate Tag')
})

it('keeps the dropdown open when focusing a tag custom-value input', async () => {
  mocks.dispatch.mockResolvedValue({ payload: { tags: [] } })
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)

  await act(async () => root.render(<Tags metadataObjectType='table' metadataObjectFullName='sales.orders' />))
  await act(async () => container.querySelector('button').click())
  const input = container.querySelector('input')

  await act(async () => input.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })))
  await act(async () => mocks.blur())
  await act(async () => input.focus())

  expect(container.querySelector('[data-testid="cascader"]')).not.toBeNull()
  expect(document.activeElement).toBe(input)
})

it('associates an unrestricted tag without a value when explicitly selected', async () => {
  mocks.dispatch.mockResolvedValue({ payload: { tags: [] } })
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)

  await act(async () => root.render(<Tags metadataObjectType='table' metadataObjectFullName='sales.orders' />))
  await act(async () => container.querySelector('button').click())
  await act(async () => mocks.change(['classification', '__no_value__']))

  expect(mocks.dispatch).toHaveBeenCalledWith({
    kind: 'associate',
    args: {
      metalake: 'demo',
      metadataObjectType: 'table',
      metadataObjectFullName: 'sales.orders',
      data: { tagsToAdd: [{ name: 'classification' }] }
    }
  })
})

it('closes the picker without requesting an already-associated value', async () => {
  mocks.tagsData = [{ name: 'classification', allowedValues: ['public', 'private'] }]
  mocks.dispatch.mockResolvedValue({
    payload: { tags: [{ name: 'classification', assignmentValues: ['public'], properties: {} }] }
  })
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)

  await act(async () => root.render(<Tags metadataObjectType='table' metadataObjectFullName='sales.orders' />))
  await act(async () =>
    [...container.querySelectorAll('button')].find(button => button.textContent.includes('Associate Tag')).click()
  )
  await act(async () => mocks.change(['classification', 'public']))
  await act(async () => mocks.blur())

  expect(container.querySelector('[data-testid="cascader"]')).toBeNull()
  expect(mocks.dispatch).not.toHaveBeenCalledWith(expect.objectContaining({ kind: 'associate' }))
})

it.each(['__no_value__', '__custom_value__'])(
  'associates %s as a literal value when it is explicitly allowed',
  async literalValue => {
    mocks.tagsData = [{ name: 'classification', allowedValues: [literalValue] }]
    mocks.dispatch.mockResolvedValue({ payload: { tags: [] } })
    container = document.createElement('div')
    document.body.appendChild(container)
    root = createRoot(container)

    await act(async () => root.render(<Tags metadataObjectType='table' metadataObjectFullName='sales.orders' />))
    await act(async () => container.querySelector('button').click())
    await act(async () => mocks.change(['classification', literalValue]))
    await act(async () => mocks.blur())

    expect(mocks.dispatch).toHaveBeenCalledWith({
      kind: 'associate',
      args: {
        metalake: 'demo',
        metadataObjectType: 'table',
        metadataObjectFullName: 'sales.orders',
        data: { tagsToAdd: [{ name: 'classification', value: literalValue }] }
      }
    })
  }
)
