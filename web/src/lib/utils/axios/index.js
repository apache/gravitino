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

// ** The axios configuration can be changed according to the project, just change the file, other files can be left unchanged

var __awaiter =
  (this && this.__awaiter) ||
  function (thisArg, _arguments, P, generator) {
    function adopt(value) {
      return value instanceof P
        ? value
        : new P(function (resolve) {
            resolve(value)
          })
    }

    return new (P || (P = Promise))(function (resolve, reject) {
      function fulfilled(value) {
        try {
          step(generator.next(value))
        } catch (e) {
          reject(e)
        }
      }
      function rejected(value) {
        try {
          step(generator['throw'](value))
        } catch (e) {
          reject(e)
        }
      }
      function step(result) {
        result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected)
      }
      step((generator = generator.apply(thisArg, _arguments || [])).next())
    })
  }

var __generator =
  (this && this.__generator) ||
  function (thisArg, body) {
    var _ = {
        label: 0,
        sent: function () {
          if (t[0] & 1) throw t[1]

          return t[1]
        },
        trys: [],
        ops: []
      },
      f,
      y,
      t,
      g

    return (
      (g = { next: verb(0), throw: verb(1), return: verb(2) }),
      typeof Symbol === 'function' &&
        (g[Symbol.iterator] = function () {
          return this
        }),
      g
    )
    function verb(n) {
      return function (v) {
        return step([n, v])
      }
    }
    function step(op) {
      if (f) throw new TypeError('Generator is already executing.')
      while ((g && ((g = 0), op[0] && (_ = 0)), _))
        try {
          if (
            ((f = 1),
            y &&
              (t = op[0] & 2 ? y['return'] : op[0] ? y['throw'] || ((t = y['return']) && t.call(y), 0) : y.next) &&
              !(t = t.call(y, op[1])).done)
          )
            return t
          if (((y = 0), t)) op = [op[0] & 2, t.value]
          switch (op[0]) {
            case 0:
            case 1:
              t = op
              break
            case 4:
              _.label++

              return { value: op[1], done: false }
            case 5:
              _.label++
              y = op[1]
              op = [0]
              continue
            case 7:
              op = _.ops.pop()
              _.trys.pop()
              continue
            default:
              if (!((t = _.trys), (t = t.length > 0 && t[t.length - 1])) && (op[0] === 6 || op[0] === 2)) {
                _ = 0
                continue
              }
              if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) {
                _.label = op[1]
                break
              }
              if (op[0] === 6 && _.label < t[1]) {
                _.label = t[1]
                t = op
                break
              }
              if (t && _.label < t[2]) {
                _.label = t[2]
                _.ops.push(op)
                break
              }
              if (t[2]) _.ops.pop()
              _.trys.pop()
              continue
          }
          op = body.call(thisArg, _)
        } catch (e) {
          op = [6, e]
          y = 0
        } finally {
          f = t = 0
        }
      if (op[0] & 5) throw op[1]

      return { value: op[0] ? op[1] : void 0, done: true }
    }
  }

import { clone } from 'lodash-es'
import { NextAxios } from './Axios'
import { checkStatus } from './checkStatus'
import toast from 'react-hot-toast'
import { RequestEnum, ResultEnum, ContentTypeEnum } from '@/lib/enums/httpEnum'
import { isString, isUndefined, isNull, isEmpty } from '@/lib/utils/is'
import { setObjToUrlParams, deepMerge } from '@/lib/utils'
import { joinTimestamp, formatRequestDate } from './helper'
import { AxiosRetry } from './axiosRetry'
import axios from 'axios'
import { useAuth as Auth } from '../../../../jsdist/src/lib/provider/session'
import qs from 'qs'

var isRefreshing = false

var refreshToken = function () {
  return __awaiter(void 0, void 0, void 0, function () {
    var url, params, res

    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0:
          url = localStorage.getItem('oauthUrl')
          params = localStorage.getItem('authParams')

          return [
            4 /*yield*/,
            defHttp.post({ url: ''.concat(url, '?').concat(qs.stringify(JSON.parse(params))) }, { withToken: false })
          ]
        case 1:
          res = _a.sent()

          return [2 /*return*/, res]
      }
    })
  })
}

var resetToLoginState = function () {
  localStorage.removeItem('accessToken')
  localStorage.removeItem('authParams')
  window.location.href = '/login'
}

/**
 * @description: Data processing to facilitate the distinction of multiple processing methods
 */
var transform = {
  /**
   * @description: Handling response data. If the data does not conform to the expected format, an error can be thrown directly
   */
  transformResponseHook: function (res, options) {
    var isTransformResponse = options.isTransformResponse,
      isReturnNativeResponse = options.isReturnNativeResponse

    // ** Whether to return the native response headers, for example: use this property when you need to access the response headers
    if (isReturnNativeResponse) {
      return res
    }

    // ** No processing is performed; return as is
    // ** Enable when it is necessary to directly access the code, data, and message information in the page code
    if (!isTransformResponse) {
      return res.data
    }
    var data = res.data
    if (!data) {
      // ** return '[HTTP] Request has no return value';
      throw new Error('The interface request failed, please try again later!')
    }

    var code = data.code,
      result = data.result,
      message = data.message
    var hasSuccess = data && Reflect.has(data, 'code') && code === ResultEnum.SUCCESS
    if (hasSuccess) {
      var successMsg = message
      if (isNull(successMsg) || isUndefined(successMsg) || isEmpty(successMsg)) {
        successMsg = 'Operation Success'
      }
      if (options.successMessageMode === 'modal') {
        console.log({ title: 'Success Tip', text: successMsg, icon: 'success' })
      } else if (options.successMessageMode === 'message') {
        toast.success(successMsg)
      }

      return result
    }
    var timeoutMsg = ''
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
  beforeRequestHook: function (config, options) {
    var _a

    var apiUrl = options.apiUrl,
      joinPrefix = options.joinPrefix,
      joinParamsToUrl = options.joinParamsToUrl,
      formatDate = options.formatDate,
      _b = options.joinTime,
      joinTime = _b === void 0 ? true : _b,
      urlPrefix = options.urlPrefix
    if (joinPrefix) {
      config.url = ''.concat(urlPrefix).concat(config.url)
    }
    if (apiUrl && isString(apiUrl)) {
      config.url = ''.concat(apiUrl).concat(config.url)
    }
    var params = config.params || {}
    var data = config.data || false
    formatDate && data && !isString(data) && formatRequestDate(data)
    if (((_a = config.method) === null || _a === void 0 ? void 0 : _a.toUpperCase()) === RequestEnum.GET) {
      if (!isString(params)) {
        // ** Add a timestamp parameter to the GET request to avoid retrieving data from the cache
        config.params = Object.assign(params || {}, joinTimestamp(joinTime, false))
      } else {
        // ** Supporting RESTful Style
        config.url = config.url + params + ''.concat(joinTimestamp(joinTime, true))
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
   * @description: Interceptor Handling of Requests
   */
  requestInterceptors: function (config, options) {
    var _a

    // ** Pre-Request Configuration Handling
    var token = localStorage.getItem('accessToken')
    if (
      token &&
      ((_a = config === null || config === void 0 ? void 0 : config.requestOptions) === null || _a === void 0
        ? void 0
        : _a.withToken) !== false
    ) {
      // ** jwt token
      config.headers.Authorization = options.authenticationScheme
        ? ''.concat(options.authenticationScheme, ' ').concat(token)
        : token
    }

    return config
  },

  /**
   * @description: Interceptor Handling of Responses
   */
  responseInterceptors: function (res) {
    return res
  },

  /**
   * @description: Error Response Handling
   */
  responseInterceptorsCatch: function (axiosInstance, error) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k

    var _l = error || {},
      response = _l.response,
      code = _l.code,
      message = _l.message,
      originConfig = _l.config

    var errorMessageMode =
      ((_a = originConfig === null || originConfig === void 0 ? void 0 : originConfig.requestOptions) === null ||
      _a === void 0
        ? void 0
        : _a.errorMessageMode) || 'none'

    var msg =
      (_f =
        (_d =
          (_c =
            (_b = response === null || response === void 0 ? void 0 : response.data) === null || _b === void 0
              ? void 0
              : _b.error) === null || _c === void 0
            ? void 0
            : _c.message) !== null && _d !== void 0
          ? _d
          : (_e = response === null || response === void 0 ? void 0 : response.data) === null || _e === void 0
            ? void 0
            : _e.message) !== null && _f !== void 0
        ? _f
        : ''

    var err =
      (_h =
        (_g = error === null || error === void 0 ? void 0 : error.toString) === null || _g === void 0
          ? void 0
          : _g.call(error)) !== null && _h !== void 0
        ? _h
        : ''
    var errMessage = ''
    if (axios.isCancel(error)) {
      return Promise.reject(error)
    }
    try {
      if (code === 'ECONNABORTED' && message.indexOf('timeout') !== -1) {
        errMessage = 'The interface request timed out, please refresh the page and try again!'
      }
      if (err === null || err === void 0 ? void 0 : err.includes('Network Error')) {
        errMessage = 'Please check if your network connection is normal! The network is abnormal'
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
    checkStatus(
      (_j = error === null || error === void 0 ? void 0 : error.response) === null || _j === void 0
        ? void 0
        : _j.status,
      msg,
      errorMessageMode
    )
    if ((response === null || response === void 0 ? void 0 : response.status) === 401 && !originConfig._retry) {
      // Log out directly if idle for more than 30 minutes
      var isIdle = localStorage.getItem('isIdle') && JSON.parse(localStorage.getItem('isIdle'))
      if (isIdle) {
        console.error('User is idle')
        resetToLoginState()
      }
      originConfig._retry = true
      if (!isRefreshing) {
        isRefreshing = true
        try {
          refreshToken()
            .then(function (res) {
              var access_token = res.access_token
              localStorage.setItem('accessToken', access_token)

              return defHttp.request(originConfig)
            })
            .catch(function (err) {
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
    var retryRequest = new AxiosRetry()

    var isOpenRetry = originConfig.requestOptions.retryRequest.isOpenRetry
    ;((_k = originConfig.method) === null || _k === void 0 ? void 0 : _k.toUpperCase()) === RequestEnum.GET &&
      isOpenRetry &&
      retryRequest.retry(axiosInstance, error)

    return Promise.reject(error)
  }
}

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

export var defHttp = createAxios()
