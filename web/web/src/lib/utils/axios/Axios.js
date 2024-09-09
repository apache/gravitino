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
 * Referred from src/utils/http/axios/Axios.ts
 */

import axios from 'axios'
import qs from 'qs'
import { AxiosCanceler } from './axiosCancel'
import { isFunction } from '../is'
import { cloneDeep } from 'lodash-es'
import { ContentTypeEnum, RequestEnum } from '../../enums/httpEnum'

export * from './axiosTransform'

/**
 * @typedef {import('axios').AxiosRequestConfig} AxiosRequestConfig
 * @typedef {import('axios').AxiosInstance} AxiosInstance
 * @typedef {import('axios').AxiosResponse} AxiosResponse
 * @typedef {import('axios').AxiosError} AxiosError
 * @typedef {import('axios').InternalAxiosRequestConfig} InternalAxiosRequestConfig
 * @typedef {import('@/types/axios').RequestOptions} RequestOptions
 * @typedef {import('@/types/axios').Result} Result
 * @typedef {import('@/types/axios').UploadFileParams} UploadFileParams
 * @typedef {import('./axiosTransform').CreateAxiosOptions} CreateAxiosOptions
 */

/**
 * @description: axios module
 */
class NextAxios {
  /**
   * @type {AxiosInstance}
   * @private
   */
  axiosInstance

  /**
   * @type {CreateAxiosOptions}
   * @readonly
   */
  options

  /**
   * @param {CreateAxiosOptions} options
   */
  constructor(options) {
    this.options = options
    this.axiosInstance = axios.create(options)
    this.setupInterceptors()
  }

  /**
   * @description: Create axios instance
   * @private
   * @param {CreateAxiosOptions} config
   */
  createAxios(config) {
    this.axiosInstance = axios.create(config)
  }

  getTransform() {
    const { transform } = this.options

    return transform
  }

  /**
   * @returns {AxiosInstance}
   */
  getAxios() {
    return this.axiosInstance
  }

  /**
   * @description: Reconfigure axios
   * @param {CreateAxiosOptions} config
   */
  configAxios(config) {
    if (!this.axiosInstance) {
      return
    }
    this.createAxios(config)
  }

  /**
   * @description: Set general header
   * @param {Object} headers
   */
  setHeader(headers) {
    if (!this.axiosInstance) {
      return
    }
    Object.assign(this.axiosInstance.defaults.headers, headers)
  }

  /**
   * @description: Interceptor configuration
   * @private
   */
  setupInterceptors() {
    const {
      axiosInstance,
      options: { transform }
    } = this
    if (!transform) {
      return
    }

    const { requestInterceptors, requestInterceptorsCatch, responseInterceptors, responseInterceptorsCatch } = transform

    const axiosCanceler = new AxiosCanceler()

    // ** Request interceptor configuration processing
    this.axiosInstance.interceptors.request.use(config => {
      // ** If cancel repeat request is turned on, then cancel repeat request is prohibited
      const requestOptions = config.requestOptions ?? this.options.requestOptions
      const ignoreCancelToken = requestOptions?.ignoreCancelToken ?? true

      !ignoreCancelToken && axiosCanceler.addPending(config)

      if (requestInterceptors && isFunction(requestInterceptors)) {
        config = requestInterceptors(config, this.options)
      }

      return config
    }, undefined)

    // ** Request interceptor error capture
    requestInterceptorsCatch &&
      isFunction(requestInterceptorsCatch) &&
      this.axiosInstance.interceptors.request.use(undefined, requestInterceptorsCatch)

    // ** Response result interceptor processing
    this.axiosInstance.interceptors.response.use(res => {
      res && axiosCanceler.removePending(res.config)
      if (responseInterceptors && isFunction(responseInterceptors)) {
        res = responseInterceptors(res)
      }

      return res
    }, undefined)

    // ** Response result interceptor error capture
    responseInterceptorsCatch &&
      isFunction(responseInterceptorsCatch) &&
      this.axiosInstance.interceptors.response.use(undefined, error => {
        return responseInterceptorsCatch(axiosInstance, error)
      })
  }

  /**
   * @description: File Upload
   * @template T
   * @param {AxiosRequestConfig} config
   * @param {UploadFileParams} params
   * @returns {Promise<T>}
   */
  uploadFile(config, params) {
    const formData = new window.FormData()
    const customFilename = params.name || 'file'

    if (params.filename) {
      formData.append(customFilename, params.file, params.filename)
    } else {
      formData.append(customFilename, params.file)
    }

    if (params.data) {
      Object.keys(params.data).forEach(key => {
        const value = params.data[key]
        if (Array.isArray(value)) {
          value.forEach(item => {
            formData.append(`${key}[]`, item)
          })

          return
        }

        formData.append(key, params.data[key])
      })
    }

    return this.axiosInstance.request({
      ...config,
      method: 'POST',
      data: formData,
      headers: {
        'Content-type': ContentTypeEnum.FORM_DATA,
        ignoreCancelToken: true
      }
    })
  }

  // ** support form-data
  supportFormData(config) {
    const headers = config.headers || this.options.headers
    const contentType = headers?.['Content-Type'] || headers?.['content-type']

    if (
      contentType !== ContentTypeEnum.FORM_URLENCODED ||
      !Reflect.has(config, 'data') ||
      config.method?.toUpperCase() === RequestEnum.GET
    ) {
      return config
    }

    return {
      ...config,
      data: qs.stringify(config.data, { arrayFormat: 'brackets' })
    }
  }

  /**
   * @template T
   * @param {AxiosRequestConfig} config
   * @param {RequestOptions} [options]
   * @returns {Promise<T>}
   */
  get(config, options) {
    return this.request({ ...config, method: 'GET' }, options)
  }

  /**
   * @template T
   * @param {AxiosRequestConfig} config
   * @param {RequestOptions} [options]
   * @returns {Promise<T>}
   */
  post(config, options) {
    return this.request({ ...config, method: 'POST' }, options)
  }

  /**
   * @template T
   * @param {AxiosRequestConfig} config
   * @param {RequestOptions} [options]
   * @returns {Promise<T>}
   */
  put(config, options) {
    return this.request({ ...config, method: 'PUT' }, options)
  }

  /**
   * @template T
   * @param {AxiosRequestConfig} config
   * @param {RequestOptions} [options]
   * @returns {Promise<T>}
   */
  delete(config, options) {
    return this.request({ ...config, method: 'DELETE' }, options)
  }

  /**
   * @template T
   * @param {AxiosRequestConfig} config
   * @param {RequestOptions} [options]
   * @returns {Promise<T>}
   */
  request(config, options) {
    let conf = cloneDeep(config)
    if (config.cancelToken) {
      conf.cancelToken = config.cancelToken
    }

    if (config.signal) {
      conf.signal = config.signal
    }

    const transform = this.getTransform()

    const { requestOptions } = this.options

    const opt = Object.assign({}, requestOptions, options)

    const { beforeRequestHook, requestCatchHook, transformResponseHook } = transform || {}
    if (beforeRequestHook && isFunction(beforeRequestHook)) {
      conf = beforeRequestHook(conf, opt)
    }
    conf.requestOptions = opt

    conf = this.supportFormData(conf)

    return new Promise((resolve, reject) => {
      this.axiosInstance
        .request(conf)
        .then(res => {
          if (transformResponseHook && isFunction(transformResponseHook)) {
            try {
              const ret = transformResponseHook(res, opt)
              resolve(ret)
            } catch (err) {
              reject(err || new Error('request error!'))
            }

            return
          }
          resolve(res)
        })
        .catch(e => {
          if (requestCatchHook && isFunction(requestCatchHook)) {
            reject(requestCatchHook(e, opt))

            return
          }
          if (axios.isAxiosError(e)) {
            // ** rewrite error message from axios in here
          }
          reject(e)
        })
    })
  }
}

export { NextAxios }
