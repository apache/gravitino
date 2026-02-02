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
  getTagsApi,
  createTagApi,
  deleteTagApi,
  updateTagApi,
  getTagDetailsApi,
  associateTagApi,
  fetchTagsForEntityApi
} from '@/lib/api/tags'

export const fetchTags = createAsyncThunk('tags/fetchTags', async ({ metalake, details }) => {
  const [err, res] = await to(getTagsApi({ metalake, details }))

  if (err || !res) {
    throw new Error(err)
  }

  return res
})

export const createTag = createAsyncThunk('tags/createTag', async ({ metalake, data }, { dispatch }) => {
  const [err, res] = await to(createTagApi({ metalake, data }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchTags({ metalake, details: true }))

  return res
})

export const getTagDetails = createAsyncThunk('tags/getTagDetails', async ({ metalake, tag }) => {
  const [err, res] = await to(getTagDetailsApi({ metalake, tag }))

  if (err || !res) {
    throw new Error(err)
  }

  return res.tag
})

export const updateTag = createAsyncThunk('tags/updateTag', async ({ metalake, tag, data }, { dispatch }) => {
  const [err, res] = await to(updateTagApi({ metalake, tag, data }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchTags({ metalake, details: true }))

  return res
})

export const deleteTag = createAsyncThunk('tags/deleteTag', async ({ metalake, tag }, { dispatch }) => {
  const [err, res] = await to(deleteTagApi({ metalake, tag }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchTags({ metalake, details: true }))

  return res
})

export const associateTag = createAsyncThunk(
  'tags/associateTag',
  async ({ metalake, metadataObjectType, metadataObjectFullName, data }) => {
    const [err, res] = await to(associateTagApi({ metalake, metadataObjectType, metadataObjectFullName, data }))

    if (err || !res) {
      throw new Error(err)
    }

    return res
  }
)

const tagsSlice = createSlice({
  name: 'tags',
  initialState: {
    tagsData: [],
    tagsLoading: false,
    error: null
  },
  reducers: {
    setTagsLoading(state, action) {
      state.tagsLoading = action.payload
    }
  },
  extraReducers: builder => {
    builder
      .addCase(fetchTags.pending, state => {
        state.tagsLoading = true
        state.error = null
      })
      .addCase(fetchTags.fulfilled, (state, action) => {
        state.tagsLoading = false
        state.tagsData = action.payload.tags || []
      })
      .addCase(fetchTags.rejected, (state, action) => {
        state.tagsLoading = false
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(createTag.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(updateTag.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(getTagDetails.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(deleteTag.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(associateTag.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
  }
})

export const { setTagsLoading } = tagsSlice.actions

export default tagsSlice.reducer
