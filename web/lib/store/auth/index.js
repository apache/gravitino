/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

import { to, isProdEnv } from '@/lib/utils'

import { getAuthConfigsApi, loginApi } from '@/lib/api/auth'

const devOauthUrl = process.env.NEXT_PUBLIC_OAUTH_PATH

export const getAuthConfigs = createAsyncThunk('sys/getAuthConfigs', async () => {
  let oauthUrl = null
  let authType = null
  const [err, res] = await to(getAuthConfigsApi())

  if (!err && res) {
    oauthUrl = `${res.data['gravitino.authenticator.oauth.serverUri']}${res.data['gravitino.authenticator.oauth.tokenPath']}`

    authType = res.data['gravitino.authenticator']
  }

  return { oauthUrl, authType }
})

export const refreshToken = createAsyncThunk('sys/refreshToken', async (data, { getState, dispatch }) => {
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

export const authSlice = createSlice({
  name: 'auth',
  initialState: {
    oauthUrl: null,
    authType: null,
    authToken: '',
    authParams: null,
    expiredIn: null
  },
  reducers: {
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
