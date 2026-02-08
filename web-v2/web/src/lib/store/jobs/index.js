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
  getJobsApi,
  getJobTemplatesApi,
  createJobTemplateApi,
  getJobTemplateApi,
  updateJobTemplateApi,
  deleteJobTemplateApi,
  createJobApi,
  getJobApi,
  cancelJobApi
} from '@/lib/api/jobs'
import { is } from '@/lib/utils/is'

export const fetchJobs = createAsyncThunk('jobs/fetchJobs', async ({ metalake }, { dispatch }) => {
  await dispatch(setJobsLoading(true))
  const [err, res] = await to(getJobsApi({ metalake }))
  await dispatch(setJobsLoading(false))

  if (err || !res) {
    throw new Error(err)
  }

  return res.jobs
})

export const fetchJobTemplates = createAsyncThunk(
  'jobs/fetchJobTemplates',
  async ({ metalake, details }, { dispatch }) => {
    await dispatch(setJobTemplatesLoading(true))
    const [err, res] = await to(getJobTemplatesApi({ metalake, details }))
    await dispatch(setJobTemplatesLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    return { jobTemplatesRes: res, details }
  }
)

export const createJobTemplate = createAsyncThunk(
  'jobs/createJobTemplate',
  async ({ metalake, data, details }, { dispatch }) => {
    const [err, res] = await to(createJobTemplateApi({ metalake, data, details }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchJobTemplates({ metalake, details }))

    return res
  }
)

export const updateJobTemplate = createAsyncThunk(
  'jobs/updateJobTemplate',
  async ({ metalake, jobTemplate, data }, { dispatch }) => {
    const [err, res] = await to(updateJobTemplateApi({ metalake, jobTemplate, data }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchJobTemplates({ metalake, details: true }))

    return res
  }
)

export const getJobTemplate = createAsyncThunk(
  'jobs/getJobTemplate',
  async ({ metalake, jobTemplate }, { dispatch }) => {
    const [err, res] = await to(getJobTemplateApi({ metalake, jobTemplate }))

    if (err || !res) {
      throw new Error(err)
    }

    return res.jobTemplate
  }
)

export const deleteJobTemplate = createAsyncThunk(
  'jobs/deleteJobTemplate',
  async ({ metalake, jobTemplate }, { dispatch }) => {
    const [err, res] = await to(deleteJobTemplateApi({ metalake, jobTemplate }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchJobTemplates({ metalake, details: true }))

    return res
  }
)

export const createJob = createAsyncThunk('jobs/createJob', async ({ metalake, data }, { dispatch }) => {
  const [err, res] = await to(createJobApi({ metalake, data }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchJobs({ metalake }))

  return res
})

export const getJobDetails = createAsyncThunk('jobs/getJobDetails', async ({ metalake, jobId }, { dispatch }) => {
  const [err, res] = await to(getJobApi({ metalake, jobId }))

  if (err || !res) {
    throw new Error(err)
  }

  return res.job
})

export const cancelJob = createAsyncThunk('jobs/cancelJob', async ({ metalake, jobId }, { dispatch }) => {
  const [err, res] = await to(cancelJobApi({ metalake, jobId }))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchJobs({ metalake }))

  return res
})

const jobsSlice = createSlice({
  name: 'jobs',
  initialState: {
    jobsData: [],
    jobTemplates: [],
    jobTemplateNames: [],
    isJobsLoading: false,
    isJobTemplatesLoading: false
  },
  reducers: {
    setJobsLoading: (state, action) => {
      state.isJobsLoading = action.payload
    },
    setJobTemplatesLoading: (state, action) => {
      state.isJobTemplatesLoading = action.payload
    }
  },
  extraReducers: builder => {
    builder
      .addCase(fetchJobs.fulfilled, (state, action) => {
        state.jobsData = action.payload
      })
      .addCase(fetchJobs.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(fetchJobTemplates.fulfilled, (state, action) => {
        const { jobTemplatesRes, details } = action.payload
        if (details) {
          state.jobTemplates = jobTemplatesRes.jobTemplates
        } else {
          state.jobTemplateNames = jobTemplatesRes.names
        }
      })
      .addCase(fetchJobTemplates.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(createJobTemplate.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(updateJobTemplate.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(getJobTemplate.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(deleteJobTemplate.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(getJobDetails.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
      .addCase(cancelJob.rejected, (state, action) => {
        if (!action.error.message.includes('CanceledError')) {
          toast.error(action.error.message)
        }
      })
  }
})

export const { setJobsLoading, setJobTemplatesLoading } = jobsSlice.actions

export default jobsSlice.reducer
