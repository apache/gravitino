/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'
import toast from 'react-hot-toast'

import { to, isProdEnv } from '@/lib/utils'

import { getAuthConfigsApi, loginApi } from '@/lib/api/auth'

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
  authType = res['gravitino.authenticator']

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

  const { access_token, expires_in } = res

  localStorage.setItem('accessToken', access_token)
  localStorage.setItem('expiredIn', expires_in)
  dispatch(setAuthToken(access_token))
  dispatch(setExpiredIn(expires_in))

  await dispatch(initialVersion())

  router.push('/')

  return { token: access_token, expired: expires_in }
})

export const logoutAction = createAsyncThunk('auth/logoutAction', async ({ router }, { getState, dispatch }) => {
  localStorage.removeItem('accessToken')
  localStorage.removeItem('authParams')
  dispatch(setAuthToken(''))
  await router.push('/ui/login')

  return { token: null }
})

export const setIntervalId = createAsyncThunk('auth/setIntervalId', async (expiredIn, { dispatch }) => {
  const localExpiredIn = localStorage.getItem('expiredIn')

  // ** the expired time obtained from the backend is in seconds, default value is 299 seconds
  const expired = (expiredIn ?? Number(localExpiredIn)) * (2 / 3) * 1000
  const defaultExpired = 299 * (2 / 3) * 1000

  let intervalId = setInterval(() => {
    dispatch(refreshToken())
  }, expired || defaultExpired)

  return {
    intervalId
  }
})

export const authSlice = createSlice({
  name: 'auth',
  initialState: {
    oauthUrl: null,
    authType: null,
    authToken: null,
    authParams: null,
    expiredIn: null,
    intervalId: null
  },
  reducers: {
    setIntervalId(state, action) {
      state.intervalId = this.setIntervalId()
    },
    clearIntervalId(state, action) {
      clearInterval(state.intervalId)
      state.intervalId = null
    },
    setAuthToken(state, action) {
      state.authToken = action.payload
    },
    setAuthParams(state, action) {
      state.authParams = action.payload
    },
    setExpiredIn(state, action) {
      state.expiredIn = action.payload
    }
  },
  extraReducers: builder => {
    builder.addCase(getAuthConfigs.fulfilled, (state, action) => {
      state.oauthUrl = action.payload.oauthUrl
      state.authType = action.payload.authType
    })
    builder.addCase(refreshToken.fulfilled, (state, action) => {
      state.authToken = action.payload.token
      state.expiredIn = action.payload.expiredIn
    })
  }
})

export const { setAuthToken, setAuthParams, setExpiredIn } = authSlice.actions

export default authSlice.reducer
