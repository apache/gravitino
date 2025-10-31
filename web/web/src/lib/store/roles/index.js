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
  getRolesApi,
  createRoleApi,
  getRoleDetailsApi,
  updateRolePrivilegesApi,
  deleteRoleApi,
  listRolesForObjectApi
} from '@/lib/api/roles'

export const fetchRoles = createAsyncThunk('roles/fetchRoles', async ({ metalake }, { dispatch }) => {
  await dispatch(setRolesLoading(true))
  const [err, res] = await to(getRolesApi({ metalake }))
  await dispatch(setRolesLoading(false))

  if (err || !res) {
    throw new Error(err)
  }

  return res.names
})

export const createRole = createAsyncThunk('roles/createRole', async ({ metalake, data }, { dispatch }) => {
  const [err, res] = await to(createRoleApi({ metalake, data }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchRoles({ metalake }))

  return res
})

export const getRoleDetails = createAsyncThunk('roles/getRoleDetails', async ({ metalake, role }) => {
  const [err, res] = await to(getRoleDetailsApi({ metalake, role }))

  if (err || !res) {
    throw new Error(err)
  }

  return res.role
})

export const updateRolePrivileges = createAsyncThunk(
  'roles/updateRolePrivileges',
  async ({ metalake, role, data }, { dispatch }) => {
    const [err, res] = await to(updateRolePrivilegesApi({ metalake, role, data }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchRoles({ metalake }))

    return res
  }
)

export const deleteRole = createAsyncThunk('roles/deleteRole', async ({ metalake, role }, { dispatch }) => {
  const [err, res] = await to(deleteRoleApi({ metalake, role }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchRoles({ metalake }))

  return res
})

export const listRolesForObject = createAsyncThunk(
  'roles/listRolesForObject',
  async ({ metalake, metadataObjectType, metadataObjectFullName }, { dispatch }) => {
    await dispatch(setRolesForObjectLoading(true))
    const [err, res] = await to(listRolesForObjectApi({ metalake, metadataObjectType, metadataObjectFullName }))
    await dispatch(setRolesForObjectLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    return res.names
  }
)

const rolesSlice = createSlice({
  name: 'roles',
  initialState: {
    rolesData: [],
    rolesLoading: false,
    rolesForObject: [],
    rolesForObjectLoading: false
  },
  reducers: {
    setRolesLoading(state, action) {
      state.rolesLoading = action.payload
    },
    setRolesForObjectLoading(state, action) {
      state.rolesForObjectLoading = action.payload
    }
  },
  extraReducers: builder => {
    builder.addCase(fetchRoles.fulfilled, (state, action) => {
      state.rolesData = action.payload
    })
    builder.addCase(fetchRoles.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getRoleDetails.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(createRole.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(updateRolePrivileges.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(deleteRole.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(listRolesForObject.fulfilled, (state, action) => {
      state.rolesForObject = action.payload || []
    })
    builder.addCase(listRolesForObject.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
  }
})

export const { setRolesLoading, setRolesForObjectLoading } = rolesSlice.actions

export default rolesSlice.reducer
