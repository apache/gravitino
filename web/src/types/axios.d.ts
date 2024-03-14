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

export type ErrorMessageMode = 'none' | 'modal' | 'message' | undefined

export type SuccessMessageMode = ErrorMessageMode

export interface RequestOptions {
  joinParamsToUrl?: boolean

  // ** Format request parameter time
  formatDate?: boolean

  // ** Whether to process the request result
  isTransformResponse?: boolean

  // ** Whether to return native response headers
  // ** For example: use this attribute when you need to get the response headers
  isReturnNativeResponse?: boolean

  // ** Whether to join url
  joinPrefix?: boolean

  // ** Interface address, use the default apiUrl if you leave it blank
  apiUrl?: string

  // ** Concatenating Path for the Request
  urlPrefix?: string

  // ** Error message prompt type
  errorMessageMode?: ErrorMessageMode

  // ** Success message prompt type
  successMessageMode?: SuccessMessageMode

  // ** Whether to add a timestamp
  joinTime?: boolean
  ignoreCancelToken?: boolean

  // ** Whether to send token in header
  withToken?: boolean

  // ** Retry Mechanism for Requests
  retryRequest?: RetryRequest
}

export interface RetryRequest {
  isOpenRetry: boolean
  count: number
  waitTime: number
}

export interface Result<T = any> {
  code: number
  type: 'success' | 'error' | 'warning'
  message: string
  result: T
}

// ** multipart/form-data: upload file
export interface UploadFileParams {
  data?: Recordable

  // ** File parameter interface field name
  name?: string

  file: File | Blob

  filename?: string
  [key: string]: any
}
