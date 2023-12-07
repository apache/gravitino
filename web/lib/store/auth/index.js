/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

import { to } from '@/lib/utils'

import { getAuthConfigsApi } from '@/lib/api/auth'

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

export const authSlice = createSlice({
  name: 'auth',
  initialState: {
    oauthUrl: null,
    authType: null
  },
  reducers: {},
  extraReducers: builder => {
    builder.addCase(getAuthConfigs.fulfilled, (state, action) => {
      state.oauthUrl = action.payload.oauthUrl
      state.authType = action.payload.authType
    })
  }
})

export default authSlice.reducer
