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
 * Referred from types/axios.d.ts
 */

/**
 * @typedef {'none' | 'modal' | 'message' | undefined} ErrorMessageMode
 */

/**
 * @typedef {ErrorMessageMode} SuccessMessageMode
 */

/**
 * @typedef {Object} RequestOptions
 * @property {boolean} [joinParamsToUrl] - Whether to join parameters to URL.
 * @property {boolean} [formatDate] - Whether to format request parameter time.
 * @property {boolean} [isTransformResponse] - Whether to process the request result.
 * @property {boolean} [isReturnNativeResponse] - Whether to return native response headers.
 * @property {boolean} [joinPrefix] - Whether to join URL prefix.
 * @property {string} [apiUrl] - The interface address.
 * @property {string} [urlPrefix] - The concatenating path for the request.
 * @property {ErrorMessageMode} [errorMessageMode] - The error message prompt type.
 * @property {SuccessMessageMode} [successMessageMode] - The success message prompt type.
 * @property {boolean} [joinTime] - Whether to add a timestamp.
 * @property {boolean} [ignoreCancelToken] - Whether to ignore cancel token.
 * @property {boolean} [withToken] - Whether to send token in header.
 * @property {RetryRequest} [retryRequest] - Retry mechanism for requests.
 */

/**
 * @typedef {Object} RetryRequest
 * @property {boolean} isOpenRetry - Whether retry mechanism is open or not.
 * @property {number} count - The number of retries.
 * @property {number} waitTime - The waiting time between retries.
 */

/**
 * @template T
 * @typedef {Object} Result
 * @property {number} code - The result code.
 * @property {'success' | 'error' | 'warning'} type - The type of result.
 * @property {string} message - The result message.
 * @property {T} result - The result data.
 */

/**
 * @typedef {Object} UploadFileParams
 * @property {Record<string, any>} [data] - The data to upload.
 * @property {string} [name] - The name of the file.
 * @property {File | Blob} file - The file to upload.
 * @property {string} [filename] - The filename.
 */

