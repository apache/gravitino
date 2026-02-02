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

import {
  getUserGroupsApi,
  createUserGroupApi,
  deleteUserGroupApi,
  grantRolesForUserGroupApi,
  revokeRolesForUserGroupApi
} from '@/lib/api/userGroups'

export const fetchUserGroups = createAsyncThunk(
  'userGroups/fetchUserGroups',
  async ({ metalake, details }, { dispatch }) => {
    await dispatch(setUserGroupsLoading(true))
    const [err, res] = await to(getUserGroupsApi({ metalake, details }))
    await dispatch(setUserGroupsLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    return { userGroupsRes: res, details }
  }
)

export const createUserGroup = createAsyncThunk(
  'userGroups/createUserGroup',
  async ({ metalake, data }, { dispatch }) => {
    const [err, res] = await to(createUserGroupApi({ metalake, data }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchUserGroups({ metalake, details: true }))

    return res
  }
)

export const deleteUserGroup = createAsyncThunk(
  'userGroups/deleteUserGroup',
  async ({ metalake, group }, { dispatch }) => {
    const [err, res] = await to(deleteUserGroupApi({ metalake, group }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchUserGroups({ metalake, details: true }))

    return res
  }
)

export const grantRolesForUserGroup = createAsyncThunk(
  'userGroups/grantRolesForUserGroup',
  async ({ metalake, group, data }, { dispatch }) => {
    const [err, res] = await to(grantRolesForUserGroupApi({ metalake, group, data }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchUserGroups({ metalake, details: true }))

    return res
  }
)

export const revokeRolesForUserGroup = createAsyncThunk(
  'userGroups/revokeRolesForUserGroup',
  async ({ metalake, group, data }, { dispatch }) => {
    const [err, res] = await to(revokeRolesForUserGroupApi({ metalake, group, data }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchUserGroups({ metalake, details: true }))

    return res
  }
)

const userGroupsSlice = createSlice({
  name: 'userGroups',
  initialState: {
    userGroupsData: [],
    userGroupNames: [],
    userGroupsLoading: false,
    error: null
  },
  reducers: {
    setUserGroupsLoading(state, action) {
      state.userGroupsLoading = action.payload
    }
  },
  extraReducers: builder => {
    builder
      .addCase(fetchUserGroups.fulfilled, (state, action) => {
        if (action.payload.details) {
          state.userGroupsData = action.payload.userGroupsRes.groups
        } else {
          state.userGroupNames = action.payload.userGroupsRes.names
        }
      })
      .addCase(fetchUserGroups.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(createUserGroup.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(deleteUserGroup.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(grantRolesForUserGroup.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(revokeRolesForUserGroup.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
  }
})

export const { setUserGroupsLoading } = userGroupsSlice.actions

export default userGroupsSlice.reducer
