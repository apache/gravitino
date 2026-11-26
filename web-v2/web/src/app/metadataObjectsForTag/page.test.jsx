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
import MetadataObjectsForTagPage from './page'

const state = vi.hoisted(() => ({ dispatch: vi.fn(), params: new URLSearchParams('metalake=demo&tag=pii') }))
vi.mock('next/navigation', () => ({ useSearchParams: () => state.params, useRouter: () => ({ back: vi.fn() }) }))
vi.mock('@/lib/hooks/useStore', () => ({ useAppDispatch: () => state.dispatch, useAppSelector: vi.fn() }))
vi.mock('@/lib/store/tags', () => ({
  getTagDetails: args => ({ kind: 'tag', args }),
  associateTag: args => ({ kind: 'associate', args })
}))
vi.mock('@/lib/store/metalakes', () => ({ getMetadataObjectsForTag: args => ({ kind: 'objects', args }) }))
vi.mock('react-antd-column-resize', () => ({
  useAntdColumnResize: getColumns => ({ resizableColumns: getColumns().columns, components: {}, tableWidth: 500 })
}))
vi.mock('antd', async () => {
  const React = await import('react')

  return {
    Flex: ({ children }) => <div>{children}</div>,
    Input: Object.assign(({ children }) => <div>{children}</div>, { Search: () => null }),
    Spin: ({ children }) => <div>{children}</div>,
    Tag: ({ children }) => <span>{children}</span>,
    Tooltip: ({ children }) => <>{children}</>,
    Typography: { Title: ({ children }) => <div>{children}</div>, Paragraph: ({ children }) => <div>{children}</div> },
    Table: ({ dataSource, columns }) =>
      React.createElement('div', null, ...dataSource.map(record => columns[2].render(null, record)))
  }
})
vi.mock('@/components/Icons', () => ({
  default: { Delete: ({ onClick }) => <button onClick={onClick}>remove</button>, Undo2: () => null }
}))
vi.mock('@/components/SectionContainer', () => ({ default: ({ children }) => <div>{children}</div> }))

let root
let container
afterEach(async () => {
  if (root) await act(async () => root.unmount())
  container?.remove()
  vi.clearAllMocks()
})

it('removes a tag association using the v1 tag name shape', async () => {
  state.dispatch.mockImplementation(async action => {
    if (action.kind === 'objects') return { payload: [{ type: 'table', fullName: 'sales.orders' }] }

    return { payload: {} }
  })
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)

  await act(async () => root.render(<MetadataObjectsForTagPage />))
  await act(async () => container.querySelector('button').click())

  expect(state.dispatch).toHaveBeenCalledWith({
    kind: 'associate',
    args: {
      metalake: 'demo',
      metadataObjectType: 'table',
      metadataObjectFullName: 'sales.orders',
      apiVersion: 1,
      data: { tagsToRemove: ['pii'] }
    }
  })
})
