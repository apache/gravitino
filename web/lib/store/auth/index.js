/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

import { to } from '@/lib/utils'

import { getAuthConfigsApi } from '@/lib/api/auth'

export const getAuthConfigs = createAsyncThunk('sys/getAuthConfigs', async () => {
  let oauthUrl = null
  const [err, res] = await to(getAuthConfigsApi())

  if (!err && res) {
    oauthUrl = `${res.data['gravitino.authenticator.oauth.serverUri']}${res.data['gravitino.authenticator.oauth.tokenPath']}`
    if (typeof window !== 'undefined') {
      window.localStorage.setItem('oauthUrl', oauthUrl)
    }
  }

  return oauthUrl
})

export const authSlice = createSlice({
  name: 'auth',
  initialState: {
    oauthUrl: null
  },
  reducers: {},
  extraReducers: builder => {}
})

export default authSlice.reducer
