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

import { to } from '@/lib/utils'
import toast from 'react-hot-toast'

import { getUsersApi, createUserApi, deleteUserApi, grantRolesForUserApi, revokeRolesForUserApi } from '@/lib/api/users'

export const fetchUsers = createAsyncThunk('users/fetchUsers', async ({ metalake, details }, { dispatch }) => {
  await dispatch(setUsersLoading(true))
  const [err, res] = await to(getUsersApi({ metalake, details }))
  await dispatch(setUsersLoading(false))

  if (err || !res) {
    throw new Error(err)
  }

  return { userRes: res, details }
})

export const createUser = createAsyncThunk('users/createUser', async ({ metalake, data }, { dispatch }) => {
  const [err, res] = await to(createUserApi({ metalake, data }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchUsers({ metalake, details: true }))

  return res
})

export const deleteUser = createAsyncThunk('users/deleteUser', async ({ metalake, user }, { dispatch }) => {
  const [err, res] = await to(deleteUserApi({ metalake, user }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchUsers({ metalake, details: true }))

  return res
})

export const grantRolesForUser = createAsyncThunk(
  'users/grantRolesForUser',
  async ({ metalake, user, data }, { dispatch }) => {
    const [err, res] = await to(grantRolesForUserApi({ metalake, user, data }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchUsers({ metalake, details: true }))

    return res
  }
)

export const revokeRolesForUser = createAsyncThunk(
  'users/revokeRolesForUser',
  async ({ metalake, user, data }, { dispatch }) => {
    const [err, res] = await to(revokeRolesForUserApi({ metalake, user, data }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchUsers({ metalake, details: true }))

    return res
  }
)

const usersSlice = createSlice({
  name: 'users',
  initialState: {
    usersData: [],
    userNames: [],
    usersLoading: false,
    error: null
  },
  reducers: {
    setUsersLoading(state, action) {
      state.usersLoading = action.payload
    }
  },
  extraReducers: builder => {
    builder
      .addCase(fetchUsers.fulfilled, (state, action) => {
        if (action.payload.details) {
          state.usersData = action.payload.userRes.users
        } else {
          state.userNames = action.payload.userRes.names
        }
      })
      .addCase(fetchUsers.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(createUser.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(deleteUser.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(grantRolesForUser.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(revokeRolesForUser.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
  }
})

export const { setUsersLoading } = usersSlice.actions

export default usersSlice.reducer
