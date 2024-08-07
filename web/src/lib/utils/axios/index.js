/*
MIT License

Copyright (c) 2020-present, Vben

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */
/**
 * Referred from src/utils/http/axios/index.ts
 */

import axios from 'axios'
import { clone } from 'lodash-es'
import toast from 'react-hot-toast'
import qs from 'qs'
import { RequestEnum, ResultEnum, ContentTypeEnum } from '@/lib/enums/httpEnum'
import { isString, isUndefined, isNull, isEmpty } from '@/lib/utils/is'
import { setObjToUrlParams, deepMerge } from '@/lib/utils'
import { joinTimestamp, formatRequestDate } from './helper'
import { AxiosRetry } from './axiosRetry'
import { AxiosCanceler } from './axiosCancel'
import { NextAxios } from './Axios'
import { checkStatus } from './checkStatus'
import { useAuth as Auth } from '../../provider/session'

let isRefreshing = false

const refreshToken = async () => {
  const url = localStorage.getItem('oauthUrl')
  const params = localStorage.getItem('authParams')

  const res = await defHttp.post({ url: `${url}?${qs.stringify(JSON.parse(params))}` }, { withToken: false })

  return res
}

const resetToLoginState = () => {
  localStorage.removeItem('accessToken')
  localStorage.removeItem('authParams')
  window.location.href = '/login'
}

/**
 * @description: Data processing to facilitate the distinction of multiple processing methods
 */
const transform = {
  /**
   * @description: Handling response data. If the data does not conform to the expected format, an error can be thrown directly
   * @param {AxiosResponse<Result>} res
   * @param {RequestOptions} options
   * @returns {any}
   */
  transformResponseHook: (res, options) => {
    const { isTransformResponse, isReturnNativeResponse } = options

    // ** Whether to return the native response headers, for example: use this property when you need to access the response headers
    if (isReturnNativeResponse) {
      return res
    }

    // ** No processing is performed; return as is
    // ** Enable when it is necessary to directly access the code, data, and message information in the page code
    if (!isTransformResponse) {
      return res.data
    }

    const { data } = res
    if (!data) {
      // ** return '[HTTP] Request has no return value';
      throw new Error('The interface request failed, please try again later!')
    }

    const { code, result, message } = data

    const hasSuccess = data && Reflect.has(data, 'code') && code === ResultEnum.SUCCESS
    if (hasSuccess) {
      let successMsg = message

      if (isNull(successMsg) || isUndefined(successMsg) || isEmpty(successMsg)) {
        successMsg = `Operation Success`
      }

      if (options.successMessageMode === 'modal') {
        console.log({ title: 'Success Tip', text: successMsg, icon: 'success' })
      } else if (options.successMessageMode === 'message') {
        toast.success(successMsg)
      }

      return result
    }

    let timeoutMsg = ''
    switch (code) {
      case ResultEnum.TIMEOUT:
        timeoutMsg = 'Login timed out, please log in again!'
        Auth().logout()
        break
      default:
        if (message) {
          timeoutMsg = message
        }
    }

    if (options.errorMessageMode === 'modal') {
      timeoutMsg && console.log({ title: 'Error Tip', text: timeoutMsg, icon: 'error' })
    } else if (options.errorMessageMode === 'message') {
      timeoutMsg && toast.error(timeoutMsg)
    }

    throw new Error(timeoutMsg || 'The interface request failed, please try again later!')
  },

  // ** Handling Configuration Prior to Request
  /**
   * @param {InternalAxiosRequestConfig} config
   * @param {RequestOptions} options
   * @returns {InternalAxiosRequestConfig}
   */
  beforeRequestHook: (config, options) => {
    const { apiUrl, joinPrefix, joinParamsToUrl, formatDate, joinTime = true, urlPrefix } = options

    if (joinPrefix) {
      config.url = `${urlPrefix}${config.url}`
    }

    if (apiUrl && isString(apiUrl)) {
      config.url = `${apiUrl}${config.url}`
    }
    const params = config.params || {}
    const data = config.data || false
    formatDate && data && !isString(data) && formatRequestDate(data)
    if (config.method?.toUpperCase() === RequestEnum.GET) {
      if (!isString(params)) {
        // ** Add a timestamp parameter to the GET request to avoid retrieving data from the cache
        config.params = Object.assign(params || {}, joinTimestamp(joinTime, false))
      } else {
        // ** Supporting RESTful Style
        config.url = config.url + params + `${joinTimestamp(joinTime, true)}`
        config.params = undefined
      }
    } else {
      if (!isString(params)) {
        formatDate && formatRequestDate(params)
        if (
          Reflect.has(config, 'data') &&
          config.data &&
          (Object.keys(config.data).length > 0 || config.data instanceof FormData)
        ) {
          config.data = data
          config.params = params
        } else {
          // ** If no data is provided for non-GET requests, the params will be treated as data
          // config.data = params
          // config.params = undefined
        }
        if (joinParamsToUrl) {
          config.url = setObjToUrlParams(config.url, Object.assign({}, config.params, config.data))
        }
      } else {
        // ** Supporting RESTful Style
        config.url = config.url + params
        config.params = undefined
      }
    }

    return config
  },

  /**
   * @param {InternalAxiosRequestConfig} config
   * @param {CreateAxiosOptions} options
   * @returns {InternalAxiosRequestConfig}
   */
  requestInterceptors: (config, options) => {
    // ** Pre-Request Configuration Handling
    const token = localStorage.getItem('accessToken')

    if (token && config?.requestOptions?.withToken !== false) {
      // ** jwt token
      config.headers.Authorization = options.authenticationScheme ? `${options.authenticationScheme} ${token}` : token
    }

    return config
  },

  /**
   * @param {AxiosResponse<any>} res
   * @returns {AxiosResponse<any>}
   */
  responseInterceptors: res => {
    return res
  },

  /**
   * @param {AxiosInstance} axiosInstance
   * @param {any} error
   * @returns {Promise<any>}
   */
  responseInterceptorsCatch: (axiosInstance, error) => {
    const { response, code, message, config: originConfig } = error || {}
    const errorMessageMode = originConfig?.requestOptions?.errorMessageMode || 'none'
    const msg = response?.data?.error?.message ?? response?.data?.message ?? ''
    const err = error?.toString?.() ?? ''
    let errMessage = ''

    if (axios.isCancel(error)) {
      return Promise.reject(error)
    }

    try {
      if (code === 'ECONNABORTED' && message.indexOf('timeout') !== -1) {
        errMessage = 'The interface request timed out, please refresh the page and try again!'
      }
      if (err?.includes('Network Error')) {
        errMessage = 'Unable to connect to Gravitino. Please check if Gravitino is running.'
      }

      if (errMessage) {
        if (errorMessageMode === 'modal') {
          console.log({ title: 'Error Tip', text: errMessage, icon: 'error' })
        } else if (errorMessageMode === 'message') {
          toast.error(errMessage)
        }

        return Promise.reject(error)
      }
    } catch (error) {
      throw new Error(error)
    }

    checkStatus(error?.response?.status, msg, errorMessageMode)

    if (response?.status === 401 && !originConfig._retry) {
      // Log out directly if idle for more than 30 minutes
      const isIdle = localStorage.getItem('isIdle') && JSON.parse(localStorage.getItem('isIdle'))
      if (isIdle) {
        console.error('User is idle')
        resetToLoginState()
      }

      originConfig._retry = true

      if (!isRefreshing) {
        isRefreshing = true

        try {
          refreshToken()
            .then(res => {
              const { access_token } = res
              localStorage.setItem('accessToken', access_token)

              return defHttp.request(originConfig)
            })
            .catch(err => {
              console.error('refreshToken error =>', err)
              resetToLoginState()
            })
        } catch (err) {
          console.error(err)
        } finally {
          isRefreshing = false
          location.reload()
        }
      }
    }

    const retryRequest = new AxiosRetry()
    const { isOpenRetry } = originConfig.requestOptions.retryRequest
    originConfig.method?.toUpperCase() === RequestEnum.GET && isOpenRetry && retryRequest.retry(axiosInstance, error)

    return Promise.reject(error)
  }
}

/**
 * @param {Partial<CreateAxiosOptions>} [opt]
 * @returns {NextAxios}
 */
function createAxios(opt) {
  return new NextAxios(
    deepMerge(
      {
        // ** See https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#authentication_schemes
        // ** authentication schemes, e.g: Bearer
        authenticationScheme: 'Bearer',
        timeout: 0,
        headers: {
          'Content-Type': ContentTypeEnum.JSON,
          Accept: 'application/vnd.gravitino.v1+json'
        },
        transform: clone(transform),

        // ** request configuration settings
        requestOptions: {
          joinPrefix: true,
          isReturnNativeResponse: false,
          isTransformResponse: false,
          joinParamsToUrl: false,
          formatDate: true,
          errorMessageMode: 'message',
          apiUrl: '',
          urlPrefix: '',
          joinTime: true,
          ignoreCancelToken: false,
          withToken: true,
          retryRequest: {
            isOpenRetry: false,
            count: 5,
            waitTime: 100
          }
        }
      },
      opt || {}
    )
  )
}

export const defHttp = createAxios()
