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
 * Referred from src/utils/http/axios/axiosRetry.ts
 */

import { AxiosError, AxiosInstance } from 'axios'

export class AxiosRetry {
  retry(axiosInstance: AxiosInstance, error: AxiosError) {
    const { config }: any = error.response
    const { waitTime, count } = config?.requestOptions?.retryRequest ?? {}
    config.__retryCount = config.__retryCount || 0
    if (config.__retryCount >= count) {
      return Promise.reject(error)
    }
    config.__retryCount += 1

    delete config.headers

    return this.delay(waitTime).then(() => axiosInstance(config))
  }

  private delay(waitTime: number) {
    return new Promise(resolve => setTimeout(resolve, waitTime))
  }
}
