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
  getPoliciesApi,
  createPolicyApi,
  deletePolicyApi,
  updatePolicyApi,
  enableOrDisablePolicyApi,
  getPolicyDetailsApi,
  associatePolicyApi
} from '@/lib/api/policies'

export const fetchPolicies = createAsyncThunk('policies/fetchPolicies', async ({ metalake, details }) => {
  const [err, res] = await to(getPoliciesApi({ metalake, details }))

  if (err || !res) {
    throw new Error(err)
  }

  return res
})

export const createPolicy = createAsyncThunk('policies/createPolicy', async ({ metalake, data }, { dispatch }) => {
  const [err, res] = await to(createPolicyApi({ metalake, data }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchPolicies({ metalake, details: true }))

  return res
})

export const getPolicyDetails = createAsyncThunk('policies/getPolicyDetails', async ({ metalake, policy }) => {
  const [err, res] = await to(getPolicyDetailsApi({ metalake, policy }))

  if (err || !res) {
    throw new Error(err)
  }

  return res.policy
})

export const updatePolicy = createAsyncThunk(
  'policies/updatePolicy',
  async ({ metalake, policy, data }, { dispatch }) => {
    const [err, res] = await to(updatePolicyApi({ metalake, policy, data }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchPolicies({ metalake, details: true }))

    return res
  }
)

export const enableOrDisablePolicy = createAsyncThunk(
  'policies/enableOrDisablePolicy',
  async ({ metalake, policy, data }, { dispatch }) => {
    const [err, res] = await to(enableOrDisablePolicyApi({ metalake, policy, data }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchPolicies({ metalake, details: true }))

    return res
  }
)

export const deletePolicy = createAsyncThunk('policies/deletePolicy', async ({ metalake, policy }, { dispatch }) => {
  const [err, res] = await to(deletePolicyApi({ metalake, policy }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchPolicies({ metalake, details: true }))

  return res
})

export const associatePolicy = createAsyncThunk(
  'policies/associatePolicy',
  async ({ metalake, metadataObjectType, metadataObjectFullName, data }) => {
    const [err, res] = await to(associatePolicyApi({ metalake, metadataObjectType, metadataObjectFullName, data }))

    if (err || !res) {
      throw new Error(err)
    }

    return res
  }
)

const policiesSlice = createSlice({
  name: 'policies',
  initialState: {
    policiesData: [],
    policiesLoading: false,
    error: null
  },
  reducers: {
    setPoliciesLoading(state, action) {
      state.policiesLoading = action.payload
    }
  },
  extraReducers: builder => {
    builder
      .addCase(fetchPolicies.pending, state => {
        state.policiesLoading = true
        state.error = null
      })
      .addCase(fetchPolicies.fulfilled, (state, action) => {
        state.policiesLoading = false
        state.policiesData = action.payload.policies || []
      })
      .addCase(fetchPolicies.rejected, (state, action) => {
        state.policiesLoading = false
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(createPolicy.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(updatePolicy.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(enableOrDisablePolicy.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(getPolicyDetails.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(deletePolicy.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(associatePolicy.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
  }
})

export const { setPoliciesLoading } = policiesSlice.actions

export default policiesSlice.reducer
