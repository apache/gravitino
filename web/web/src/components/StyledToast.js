/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

'use client'

import { toast, ToastBar, Toaster } from 'react-hot-toast'
import themeConfig from '../configs/themeConfig'

export default function StyledToast() {
  const { toastPosition, toastDuration } = themeConfig

  return (
    <div>
      <Toaster
        reverseOrder={false}
        position={toastPosition}
        toastOptions={{
          duration: toastDuration,
          style: {
            borderRadius: '8px',
            maxWidth: 500
          }
        }}
      >
        {t => (
          <ToastBar toast={t}>
            {({ icon, message }) => (
              <>
                {icon}
                <div className='twc-flex twc-w-full twc-break-normal [&>div]:twc-justify-start'>{message}</div>
                <div className='twc-flex twc-h-full'>
                  {t.type !== 'loading' && (
                    <button
                      className='twc-border-0 twc-text-[#666] twc-w-6 twc-h-6 twc-justify-start twc-rounded-full twc-ring-primary-400 twc-transition hover:twc-bg-[#f8f8f8] focus:twc-outline-none focus-visible:twc-ring'
                      onClick={() => toast.dismiss(t.id)}
                    >
                      x
                    </button>
                  )}
                </div>
              </>
            )}
          </ToastBar>
        )}
      </Toaster>
    </div>
  )
}
