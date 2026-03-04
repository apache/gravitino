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
 * Referred from src/utils/http/axios/helper.ts
 */

import { isObject, isString } from '@/lib/utils/is'

const DATE_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'

/**
 * @template T
 * @param {boolean} join
 * @param {T} restful
 * @returns {T extends true ? string : object}
 */
export function joinTimestamp(join, restful = false) {
  if (!join) {
    return restful ? '' : {}
  }
  const now = new Date().getTime()
  if (restful) {
    return `?_t=${now}`
  }

  return { _t: now }
}

/**
 * @typedef {Object.<string, any>} Recordable
 */

/**
 * @description: Format request parameter time
 * @param {Recordable} params
 */
export function formatRequestDate(params) {
  if (Object.prototype.toString.call(params) !== '[object Object]') {
    return
  }

  for (const key in params) {
    const format = params[key]?.format ?? null
    if (format && typeof format === 'function') {
      params[key] = params[key].format(DATE_TIME_FORMAT)
    }
    if (isString(key)) {
      const value = params[key]
      if (value) {
        try {
          params[key] = isString(value) ? value.trim() : value
        } catch (error) {
          throw new Error(error)
        }
      }
    }
    if (isObject(params[key])) {
      formatRequestDate(params[key])
    }
  }
}
