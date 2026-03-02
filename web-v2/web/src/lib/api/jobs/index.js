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

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/jobs/runs`,
  GET_TEMPLATE: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/jobs/templates`,
  GET_DETAIL: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}`,
  CREATE: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/jobs/runs`,
  REGISTER_JOB_TEMPLATE: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/jobs/templates`,
  UPDATE_JOB_TEMPLATE: ({ metalake, jobTemplate }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/jobs/templates/${encodeURIComponent(jobTemplate)}`,
  GET_TEMPLATE_DETAIL: ({ metalake, jobTemplate }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/jobs/templates/${encodeURIComponent(jobTemplate)}`,
  DELETE_TEMPLATE: ({ metalake, jobTemplate }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/jobs/templates/${encodeURIComponent(jobTemplate)}`,
  GET_JOB_DETAIL: ({ metalake, jobId }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/jobs/runs/${encodeURIComponent(jobId)}`,
  CANCEL_JOB: ({ metalake, jobId }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/jobs/runs/${encodeURIComponent(jobId)}`
}

export const getJobsApi = ({ metalake }) => {
  return defHttp.get({
    url: Apis.GET({ metalake })
  })
}

export const getJobTemplatesApi = ({ metalake, details }) => {
  return defHttp.get({
    url: Apis.GET_TEMPLATE({ metalake }),
    params: { details }
  })
}

export const createJobTemplateApi = ({ metalake, data }) => {
  return defHttp.post({
    url: Apis.REGISTER_JOB_TEMPLATE({ metalake }),
    data
  })
}

export const updateJobTemplateApi = ({ metalake, jobTemplate, data }) => {
  return defHttp.put({
    url: Apis.UPDATE_JOB_TEMPLATE({ metalake, jobTemplate }),
    data
  })
}

export const getJobTemplateApi = ({ metalake, jobTemplate }) => {
  return defHttp.get({
    url: Apis.GET_TEMPLATE_DETAIL({ metalake, jobTemplate })
  })
}

export const deleteJobTemplateApi = ({ metalake, jobTemplate }) => {
  return defHttp.delete({
    url: Apis.DELETE_TEMPLATE({ metalake, jobTemplate })
  })
}

export const createJobApi = ({ metalake, data }) => {
  return defHttp.post({
    url: Apis.CREATE({ metalake }),
    data
  })
}

export const getJobApi = ({ metalake, jobId }) => {
  return defHttp.get({
    url: Apis.GET_JOB_DETAIL({ metalake, jobId })
  })
}

export const cancelJobApi = ({ metalake, jobId }) => {
  return defHttp.post({
    url: Apis.CANCEL_JOB({ metalake, jobId })
  })
}
