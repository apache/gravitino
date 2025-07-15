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

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'
import toast from 'react-hot-toast'

import { to, isProdEnv } from '@/lib/utils'

import { getAuthConfigsApi, loginApi, getOAuthConfigsApi } from '@/lib/api/auth'

import { initialVersion } from '@/lib/store/sys'

const devOauthUrl = process.env.NEXT_PUBLIC_OAUTH_PATH

export const getAuthConfigs = createAsyncThunk('auth/getAuthConfigs', async () => {
  let oauthUrl = null
  let authType = null
  const [err, res] = await to(getAuthConfigsApi())

  if (err || !res) {
    throw new Error(err)
  }

  oauthUrl = `${res['gravitino.authenticator.oauth.serverUri']}${res['gravitino.authenticator.oauth.tokenPath']}`

  // ** get the first authenticator from the response. response example: "[simple, oauth]"
  authType = res['gravitino.authenticators'][0].trim()

  localStorage.setItem('oauthUrl', oauthUrl)

  return { oauthUrl, authType }
})

export const refreshToken = createAsyncThunk('auth/refreshToken', async (data, { getState, dispatch }) => {
  const authParams = getState().auth.authParams || window.localStorage.getItem('authParams')
  let params = typeof authParams === 'string' ? JSON.parse(authParams) : authParams

  const url = isProdEnv ? getState().auth.oauthUrl : devOauthUrl

  const [err, res] = await to(loginApi(url, params))

  if (err || !res) {
    throw new Error(err)
  }

  const { access_token, expires_in } = res.data

  return { token: access_token, expiredIn: expires_in }
})

export const loginAction = createAsyncThunk('auth/loginAction', async ({ params, router }, { getState, dispatch }) => {
  dispatch(setAuthParams(params))
  localStorage.setItem('authParams', JSON.stringify(params))

  const url = getState().auth.oauthUrl

  const [err, res] = await to(loginApi(url, params))

  if (err || !res) {
    toast.error(err.response?.data?.err || err.message, { id: `global_error_message_status_${err.response?.status}` })
    throw new Error(err)
  }

  const { access_token, expires_in } = res // `expires_in ` is in seconds, default is 499

  localStorage.setItem('accessToken', access_token)
  localStorage.setItem('expiredIn', expires_in)
  localStorage.setItem('isIdle', false)
  dispatch(setAuthToken(access_token))
  dispatch(setExpiredIn(expires_in))
  await dispatch(initialVersion())
  router.push('/metalakes')

  return { token: access_token, expired: expires_in }
})

export const logoutAction = createAsyncThunk('auth/logoutAction', async ({ router }, { getState, dispatch }) => {
  console.log('Logging out...')
  localStorage.removeItem('accessToken')
  localStorage.removeItem('authParams')
  dispatch(clearIntervalId())
  dispatch(setAuthToken(''))
  await router.push('/login')

  return { token: null }
})

export const setIntervalIdAction = createAsyncThunk('auth/setIntervalIdAction', async (expiredIn, { dispatch }) => {
  const localExpiredIn = localStorage.getItem('expiredIn')
  const expired = (expiredIn ?? Number(localExpiredIn)) * (2 / 3) * 1000
  const defaultExpired = 299 * (2 / 3) * 1000

  let intervalId = setInterval(() => {
    if (localStorage.getItem('isIdle') === 'true') {
      console.log('User is idle, skipping token refresh')
      localStorage.removeItem('accessToken')
      localStorage.removeItem('authParams')
      dispatch(clearIntervalId())
      dispatch(setAuthToken(''))

      return
    }
    dispatch(refreshToken())
  }, expired || defaultExpired)

  dispatch(setIntervalId(intervalId))

  return {
    intervalId
  }
})

export const getOAuthConfig = createAsyncThunk('auth/getOAuthConfig', async () => {
  const [err, res] = await to(getOAuthConfigsApi())
  if (err || !res) {
    throw new Error(err?.message || 'Failed to fetch OAuth configs')
  }

  return {
    providerName: res.providerName,
    authorizationCodeFlowEnabled: res.authorizationCodeFlowEnabled
  }
})

export const handleOAuthCallback = createAsyncThunk(
  'auth/handleOAuthCallback',
  async ({ access_token, refresh_token, expires_in, router }, { dispatch }) => {
    if (!access_token) {
      throw new Error('No OAuth access token received')
    }
    console.log('OAuth Callback received access_token:', access_token)
    if (refresh_token) {
      console.log('OAuth Callback received refresh_token:', refresh_token)
      localStorage.setItem('refresh_token', refresh_token)
    }

    // Store the OAuth access token
    localStorage.setItem('accessToken', access_token)
    if (expires_in) {
      localStorage.setItem('expiredIn', expires_in)
      dispatch(setExpiredIn(expires_in))
    }
    localStorage.setItem('isIdle', false)
    dispatch(setAuthToken(access_token))

    // Initialize version and redirect to main app
    await dispatch(initialVersion())
    router.push('/metalakes')

    return { token: access_token, expired: expires_in }
  }
)

export const authSlice = createSlice({
  name: 'auth',
  initialState: {
    oauthUrl: null,
    authType: null,
    authToken: null,
    authParams: null,
    expiredIn: null,
    intervalId: null,

    // OAuth Authorization Code Flow state
    oauthConfig: null,
    oauthSupported: false,
    oauthRedirecting: false
  },
  reducers: {
    setIntervalId(state, action) {
      state.intervalId = action.payload
    },
    clearIntervalId(state) {
      if (state.intervalId) {
        clearInterval(state.intervalId)
        state.intervalId = null
      }
    },
    setAuthToken(state, action) {
      state.authToken = action.payload
    },
    setAuthParams(state, action) {
      state.authParams = action.payload
    },
    setExpiredIn(state, action) {
      state.expiredIn = action.payload
    },
    setOAuthRedirecting(state, action) {
      state.oauthRedirecting = action.payload
    }
  },
  extraReducers: builder => {
    builder.addCase(getAuthConfigs.fulfilled, (state, action) => {
      state.oauthUrl = action.payload.oauthUrl
      state.authType = action.payload.authType
    })
    builder.addCase(refreshToken.fulfilled, (state, action) => {
      console.log('Token refreshed successfully:', action.payload.token)
      localStorage.setItem('accessToken', action.payload.token)
      localStorage.setItem('expiredIn', action.payload.expiredIn)
      localStorage.setItem('isIdle', false)
      state.authToken = action.payload.token
      state.expiredIn = action.payload.expiredIn
    })

    // OAuth Authorization Code Flow reducers
    builder.addCase(getOAuthConfig.fulfilled, (state, action) => {
      state.oauthConfig = action.payload
      state.oauthSupported = action.payload.authorizationCodeFlowEnabled
    })
    builder.addCase(getOAuthConfig.rejected, state => {
      state.oauthSupported = false
    })
    builder.addCase(handleOAuthCallback.fulfilled, (state, action) => {
      state.authToken = action.payload.token
      state.oauthRedirecting = false
    })
    builder.addCase(handleOAuthCallback.rejected, state => {
      state.oauthRedirecting = false
    })
  }
})

export const { setAuthToken, setAuthParams, setExpiredIn, clearIntervalId, setOAuthRedirecting } = authSlice.actions

export default authSlice.reducer
