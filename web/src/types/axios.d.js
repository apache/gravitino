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

export const ErrorMessageMode = 'none' | 'modal' | 'message' | undefined;

export const SuccessMessageMode = ErrorMessageMode;

export const RequestOptions = {
  joinParamsToUrl: false,

  // ** Format request parameter time
  formatDate: false,

  // ** Whether to process the request result
  isTransformResponse: false,

  // ** Whether to return native response headers
  // ** For example: use this attribute when you need to get the response headers
  isReturnNativeResponse: false,

  // ** Whether to join url
  joinPrefix: false,

  // ** interface address, use the default apiUrl if you leave it blank
  apiUrl: undefined,

  // ** Concatenating Path for the Request
  urlPrefix: undefined,

  // ** Error message prompt type
  errorMessageMode: ErrorMessageMode,

  // ** Success message prompt type
  successMessageMode: SuccessMessageMode,

  // ** Whether to add a timestamp
  joinTime: boolean,
  ignoreCancelToken: boolean,

  // ** Whether to send token in header
  withToken: boolean,

  // ** Retry Mechanism for Requests
  retryRequest: RetryRequest
}

export const RetryRequest = {
  isOpenRetry: false,
  count: 0,
  waitTime: 0,
}

export const Result = {
  code: number,
  type: 'success' | 'error' | 'warning',
  message: string,
  result: null
}

// ** multipart/form-data: upload file
export const UploadFileParams = {
  data: {},
  name: undefined,
  file: null,
  filename: undefined,
};
