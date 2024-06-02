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
 * @typedef {'none' | 'modal' | 'message' | undefined} ErrorMessageMode
 */

/**
 * @typedef {ErrorMessageMode} SuccessMessageMode
 */

/**
 * @typedef {Object} RequestOptions
 * @property {boolean} [joinParamsToUrl]
 * @property {boolean} [formatDate]
 * @property {boolean} [isTransformResponse]
 * @property {boolean} [isReturnNativeResponse]
 * @property {boolean} [joinPrefix]
 * @property {string} [apiUrl]
 * @property {string} [urlPrefix]
 * @property {ErrorMessageMode} [errorMessageMode]
 * @property {SuccessMessageMode} [successMessageMode]
 * @property {boolean} [joinTime]
 * @property {boolean} [ignoreCancelToken]
 * @property {boolean} [withToken]
 * @property {RetryRequest} [retryRequest]
 */

/**
 * @typedef {Object} RetryRequest
 * @property {boolean} isOpenRetry
 * @property {number} count
 * @property {number} waitTime
 */

/**
 * @typedef {Object} Result
 * @property {number} code
 * @property {'success' | 'error' | 'warning'} type
 * @property {string} message
 * @property {any} result
 */

/**
 * @typedef {Object} UploadFileParams
 * @property {Object.<string, any>} [data]
 * @property {string} [name]
 * @property {File|Blob} file
 * @property {string} [filename]
 * @property {Object.<string, any>} [key]
 */

