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

import { loggerVersion, to, formatNumber, loggerGitHubInfo } from '@/lib/utils'

import { getVersionApi } from '@/lib/api/version'

import { getGitHubApi } from '@/lib/api/github'

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

export const fetchGitHubInfo = createAsyncThunk('sys/fetchGitHubInfo', async (params, { getState }) => {
  let stars = 0
  let forks = 0
  const [err, res] = await to(getGitHubApi())
  if (err || !res) {
    console.error('Error fetching repository status : ', err)
  }

  stars = formatNumber(res.stargazers_count)
  forks = formatNumber(res.forks_count)
  loggerGitHubInfo(stars, forks)

  return {
    stars: stars,
    forks: forks,
  }
})

export const sysSlice = createSlice({
  name: 'sys',
  initialState: {
    version: '',
    stars: 0,
    forks: 0,
  },
  reducers: {
    setVersion(state, action) {
      state.version = action.payload
    },
    setStars(state, action) {
      state.stars = action.payload
    },
    setForks(state, action) {
      state.forks = action.payload
    },
  },
  extraReducers: builder => {
    builder
      .addCase(initialVersion.fulfilled, (state, action) => {
        state.version = action.payload.version
      })
      .addCase(fetchGitHubInfo.fulfilled, (state, action) => {
        state.stars = action.payload.stars
        state.forks = action.payload.forks
      })
  },
})

export const { setVersion, setStars, setForks } = sysSlice.actions

export default sysSlice.reducer
