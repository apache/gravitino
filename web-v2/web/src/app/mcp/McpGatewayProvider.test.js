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
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const store = { auth: {} }
const getMe = vi.fn()
const gateway = vi.hoisted(() => ({ onSignedOut: undefined }))

// Next.js returns the same router on every render.
const router = vi.hoisted(() => ({ push: vi.fn() }))
vi.mock('next/navigation', () => ({ useRouter: () => router }))
vi.mock('@/lib/hooks/useStore', () => ({ useAppSelector: select => select(store) }))
vi.mock('@/components/SectionContainer', () => ({ default: ({ children }) => children }))
vi.mock('@/lib/api/mcpGateway', async importOriginal => ({
  ...(await importOriginal()),
  createMcpGatewayApi: (uri, getAccessToken, onSignedOut) => {
    gateway.onSignedOut = onSignedOut

    return { getMe }
  }
}))

const { default: McpGatewayProvider, AdminOnly } = await import('./McpGatewayProvider')

let container
let root

const show = children =>
  act(async () => {
    root.render(createElement(McpGatewayProvider, null, children))
  })

const page = createElement(AdminOnly, null, 'admin page')

beforeEach(() => {
  globalThis.IS_REACT_ACT_ENVIRONMENT = true
  getMe.mockReset()
  router.push.mockReset()
  container = document.createElement('div')
  root = createRoot(container)
  store.auth = {
    authType: 'oauth',
    systemConfig: {
      'gravitino.mcpGateway.uri': 'https://gw.example.com',
      'gravitino.authenticator.oauth.provider': 'oidc'
    }
  }
})

afterEach(() => act(() => root.unmount()))

describe('AdminOnly', () => {
  it('shows the page to a gateway admin', async () => {
    getMe.mockResolvedValue({ username: 'alice', is_admin: true })

    await show(page)

    expect(container.textContent).toBe('admin page')
  })

  it('shows anyone else that the page is for admins', async () => {
    getMe.mockResolvedValue({ username: 'bob', is_admin: false })

    await show(page)

    expect(container.textContent).not.toContain('admin page')
    expect(container.textContent).toContain('Only MCP gateway admins can use this page')
  })
})

describe('McpGatewayProvider', () => {
  it('says the gateway is not set up when Gravitino names none', async () => {
    store.auth = { authType: 'oauth', systemConfig: {} }

    await show(page)

    expect(container.textContent).toContain('The MCP gateway is not set up')
  })

  it('needs an OIDC sign-in to Gravitino', async () => {
    store.auth = { ...store.auth, authType: 'simple' }

    await show(page)

    expect(container.textContent).toContain('MCP servers need an OIDC sign-in')
    expect(getMe).not.toHaveBeenCalled()
  })

  it("doesn't send the token of another OAuth sign-in to the gateway", async () => {
    store.auth = {
      ...store.auth,
      systemConfig: { ...store.auth.systemConfig, 'gravitino.authenticator.oauth.provider': 'default' }
    }

    await show(page)

    expect(container.textContent).toContain('MCP servers need an OIDC sign-in')
    expect(getMe).not.toHaveBeenCalled()
  })

  it('goes to the login page when a call finds nobody signed in', async () => {
    getMe.mockResolvedValue({ is_admin: true })
    await show(page)

    // As when renewing the sign-in fails after the pages have loaded.
    gateway.onSignedOut()

    expect(router.push).toHaveBeenCalledWith('/login')
  })

  it('says the gateway is unavailable when it does not answer', async () => {
    getMe.mockRejectedValue(new Error('Network Error'))

    await show(page)

    expect(container.textContent).toContain('The MCP gateway is unavailable')
  })

  it('tries the gateway again on Retry', async () => {
    getMe.mockRejectedValueOnce(new Error('Network Error')).mockResolvedValue({ is_admin: true })
    await show(page)

    const retry = [...container.querySelectorAll('button')].find(b => b.textContent === 'Retry')
    await act(async () => retry.click())

    expect(getMe).toHaveBeenCalledTimes(2)
    expect(container.textContent).toBe('admin page')
  })
})
