'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { useMsal } from '@azure/msal-react'

export default function OAuthCallbackPage() {
  const router = useRouter()
  const { accounts } = useMsal()

  useEffect(() => {
    if (accounts.length > 0) {
      console.log('[OAuthCallbackPage] User is authenticated, redirecting to /metalakes')
      router.replace('/metalakes')
    } else {
      console.warn('[OAuthCallbackPage] No accounts found, redirecting to /ui/login')
      router.replace('/ui/login')
    }
  }, [accounts, router])

  return (
    <div style={{ textAlign: 'center', marginTop: '2rem' }}>
      <h2>Completing OAuth login...</h2>
      <p>
        If you are not redirected automatically, <a href='/ui/login'>click here</a>.
      </p>
    </div>
  )
}
