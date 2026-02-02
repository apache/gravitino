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
 * Referred from src/utils/http/axios/axiosTransform.ts
 */

/**
 * @typedef {import('axios').AxiosInstance} AxiosInstance
 * @typedef {import('axios').AxiosRequestConfig} AxiosRequestConfig
 * @typedef {import('axios').AxiosResponse} AxiosResponse
 * @typedef {import('axios').InternalAxiosRequestConfig} InternalAxiosRequestConfig
 * @typedef {import('@/types/axios').RequestOptions} RequestOptions
 * @typedef {import('@/types/axios').Result} Result
 */

/**
 * @typedef {AxiosRequestConfig} CreateAxiosOptions
 * @property {string} [authenticationScheme]
 * @property {AxiosTransform} [transform]
 * @property {RequestOptions} [requestOptions]
 */

/**
 * Data processing class, can be configured according to the project
 */
class AxiosTransform {
  /**
   * A function that is called before a request is sent. It can modify the request configuration as needed.
   * @param {AxiosRequestConfig} config
   * @param {RequestOptions} options
   * @returns {AxiosRequestConfig}
   */
  beforeRequestHook(config, options) {}

  /**
   * Processing response data
   * @param {AxiosResponse<Result>} res
   * @param {RequestOptions} options
   * @returns {any}
   */
  transformResponseHook(res, options) {}

  /**
   * Handling of failed requests
   * @param {Error} e
   * @param {RequestOptions} options
   * @returns {Promise<any>}
   */
  requestCatchHook(e, options) {}

  /**
   * Interceptor before request
   * @param {InternalAxiosRequestConfig} config
   * @param {CreateAxiosOptions} options
   * @returns {InternalAxiosRequestConfig}
   */
  requestInterceptors(config, options) {}

  /**
   * Interceptor after request
   * @param {AxiosResponse<any>} res
   * @returns {AxiosResponse<any>}
   */
  responseInterceptors(res) {}

  /**
   * Interceptors error handing before request
   * @param {Error} error
   */
  requestInterceptorsCatch(error) {}

  /**
   * Interceptors error handing after request
   * @param {AxiosInstance} axiosInstance
   * @param {Error} error
   */
  responseInterceptorsCatch(axiosInstance, error) {}
}

export { AxiosTransform }
