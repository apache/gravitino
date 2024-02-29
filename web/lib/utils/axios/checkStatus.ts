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
 * Referred from src/utils/http/axios/checkStatus.ts
 */

import type { ErrorMessageMode } from '@/types/axios'
import toast from 'react-hot-toast'

export function checkStatus(status: number, msg: string, errorMessageMode: ErrorMessageMode = 'message'): void {
  let errMessage = ''

  switch (status) {
    case 400:
      errMessage = `${msg}`
      break

    case 401:
      // ** reserve error message
      errMessage = msg || 'The user does not have permission (token, user name, password error or expired)!'

      localStorage.removeItem('accessToken')
      localStorage.removeItem('authParams')

      window.location.href = '/ui/login'

      break
    case 403:
      errMessage = 'The user is authorized, but access is forbidden!'
      break

    case 404:
      errMessage = 'Network request error, the resource was not found!'
      break

    case 405:
      errMessage = 'Network request error, request method not allowed!'
      break

    case 408:
      errMessage = 'Network request timed out!'
      break

    case 409:
      errMessage = msg || 'Conflict with the current resource state!'
      break

    case 500:
      errMessage = msg || 'Server error, unable to connect Gravitino!'
      break

    case 501:
      errMessage = 'The network is not implemented!'
      break

    case 502:
      errMessage = 'Network Error!'
      break

    case 503:
      errMessage = 'The service is unavailable, the server is temporarily overloaded or maintained!'
      break

    case 504:
      errMessage = 'Network timeout!'
      break

    case 505:
      errMessage = 'The http version does not support the request!'
      break

    default:
  }

  if (errMessage) {
    if (errorMessageMode === 'modal') {
      console.log({ title: 'Error Tip', text: errMessage, icon: 'error' })
    } else if (errorMessageMode === 'message') {
      const keyword = 'reason'
      const idx = errMessage.indexOf(keyword)

      if (idx !== -1) {
        errMessage = errMessage.substring(idx + keyword.length + 1).replace(/^\[|\]$/g, '')
      }

      toast.error(errMessage, { id: `global_error_message_status_${status}` })
    }
  }
}
