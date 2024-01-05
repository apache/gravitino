/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

import { loggerVersion, to } from '@/lib/utils'

import { getVersionApi } from '@/lib/api/version'

export const initialVersion = createAsyncThunk('sys/fetchVersion', async (params, { getState }) => {
  let version = null
  const [err, res] = await to(getVersionApi())

  if (err || !res) {
    throw new Error(err)
  }

  version = res.version
  typeof window !== 'undefined' && window.localStorage.setItem('version', JSON.stringify(version))

  loggerVersion(version.version)

  return version
})

export const sysSlice = createSlice({
  name: 'sys',
  initialState: {
    version: ''
  },
  reducers: {
    setVersion(state, action) {
      state.version = action.payload
    }
  },
  extraReducers: builder => {
    builder.addCase(initialVersion.fulfilled, (state, action) => {
      state.version = action.payload.version
    })
  }
})

export const { setVersion } = sysSlice.actions

export default sysSlice.reducer
