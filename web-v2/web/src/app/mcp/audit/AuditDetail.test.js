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

import { act, createElement } from 'react'
import { createRoot } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'

const getAuditLog = vi.fn()
vi.mock('../McpGatewayProvider', () => ({ useMcpGateway: () => ({ api: { getAuditLog } }) }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))

// The drawer itself isn't under test, and jsdom can't measure it.
vi.mock('antd', async importOriginal => ({ ...(await importOriginal()), Drawer: ({ children }) => children }))

const { default: AuditDetail } = await import('./AuditDetail')

let root
let fail

beforeEach(() => {
  globalThis.IS_REACT_ACT_ENVIRONMENT = true
  getAuditLog.mockReset().mockReturnValue(
    new Promise((_, reject) => {
      fail = reject
    })
  )
  root = createRoot(document.createElement('div'))
})

afterEach(() => act(() => root.unmount()))

const open = onClose => act(async () => root.render(createElement(AuditDetail, { logId: 7, onClose })))

it('closes when the entry fails to load', async () => {
  const onClose = vi.fn()
  await open(onClose)

  await act(async () => fail(new Error('Network Error')))

  expect(getAuditLog).toHaveBeenCalledWith(7)
  expect(onClose).toHaveBeenCalledTimes(1)
})

it("ignores a failure that comes after the entry closed, so it can't close the next one", async () => {
  const onClose = vi.fn()
  await open(onClose)
  act(() => root.unmount())
  root = createRoot(document.createElement('div'))

  await act(async () => fail(new Error('Network Error')))

  expect(onClose).not.toHaveBeenCalled()
})
