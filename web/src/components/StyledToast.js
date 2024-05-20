/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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
                <div className='twc-flex twc-w-full twc-break-all [&>div]:twc-justify-start'>{message}</div>
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
