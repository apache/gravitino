'use client'

import { MsalProvider } from '@azure/msal-react'
import { initMsal } from '@/lib/auth/msal'
import React, { useEffect, useState } from 'react'

export default function MsalProviderGate({ children }) {
  const [instance, setInstance] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let mounted = true
    initMsal()
      .then(instance => {
        if (mounted) {
          setInstance(instance)
          setLoading(false)
        }
      })
      .catch(err => {
        if (mounted) {
          setError(err)
          setLoading(false)
        }
      })

    return () => {
      mounted = false
    }
  }, [])

  if (loading || !instance) {
    if (error) {
      return <div style={{ color: 'red' }}>Authentication failed: {error.message || String(error)}</div>
    }

    return <div>Loading authentication...</div>
  }

  return <MsalProvider instance={instance}>{children}</MsalProvider>
}
