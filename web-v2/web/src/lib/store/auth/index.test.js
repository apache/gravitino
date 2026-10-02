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

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { configureStore } from '@reduxjs/toolkit'

import { loginApi } from '@/lib/api/auth'
import authReducer, { getAuthMe, refreshToken, setAuthUser } from '@/lib/store/auth'

// The store reaches `@/lib/utils/axios`, which imports the JSX provider in
// `@/lib/provider/session.js`. Mock the request layer so these tests stay
// independent of it.
vi.mock('@/lib/api/auth', () => ({
  getAuthConfigsApi: vi.fn(),
  getAuthMeApi: vi.fn(),
  loginApi: vi.fn(),
  basicLoginApi: vi.fn()
}))

vi.mock('@/lib/store/sys', () => ({
  initialVersion: vi.fn(() => ({ type: 'sys/initialVersion/mock' }))
}))

describe('auth store', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
  })

  it('stores the server-resolved principal and service-admin status', () => {
    let state = authReducer(undefined, setAuthUser({ email: 'user@example.com', name: 'token-user' }))

    state = authReducer(state, getAuthMe.fulfilled({ principal: 'mapped-user', serviceAdmin: true }, 'request-id'))

    expect(state.authUser).toEqual({
      email: 'user@example.com',
      name: 'mapped-user',
      type: 'user'
    })
    expect(state.isServiceAdmin).toBe(true)
    expect(JSON.parse(sessionStorage.getItem('simpleAuthUser'))).toEqual(state.authUser)
  })

  it('clears service-admin status when the lookup fails or the user logs out', () => {
    let state = authReducer(undefined, getAuthMe.fulfilled({ principal: 'admin', serviceAdmin: true }, 'request-id'))

    state = authReducer(state, { type: getAuthMe.rejected.type })
    expect(state.isServiceAdmin).toBe(false)

    state = authReducer(state, getAuthMe.fulfilled({ principal: 'admin', serviceAdmin: true }, 'request-id'))
    state = authReducer(state, setAuthUser(null))
    expect(state.isServiceAdmin).toBe(false)
  })
})

describe('refreshToken', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    vi.clearAllMocks()
  })

  it('reads the token from the response body, as the login flow does', async () => {
    // `loginApi` resolves to the response body, not to the Axios envelope.
    loginApi.mockResolvedValue({ access_token: 'refreshed-token', expires_in: 499 })

    const store = configureStore({ reducer: { auth: authReducer } })

    const action = await store.dispatch(refreshToken())

    expect(action.type).toBe(refreshToken.fulfilled.type)
    expect(action.payload).toEqual({ token: 'refreshed-token', expiredIn: 499 })
    expect(store.getState().auth.authToken).toBe('refreshed-token')
    expect(localStorage.getItem('accessToken')).toBe('refreshed-token')
    expect(localStorage.getItem('expiredIn')).toBe('499')
  })
})
