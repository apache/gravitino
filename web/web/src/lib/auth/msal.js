'use client'

import { PublicClientApplication } from '@azure/msal-browser'

let msalConfig = null
let configPromise = null
let msalInstance = null

export async function initMsal() {
  if (msalInstance) {
    console.log('[MSAL] Reusing existing instance')

    return msalInstance
  }

  if (configPromise) {
    console.log('[MSAL] Config fetch in progress...')
    await configPromise

    return msalInstance
  }

  configPromise = (async () => {
    try {
      console.log('[MSAL] Fetching /configs...')
      const response = await fetch('/configs')
      if (!response.ok) throw new Error(`Failed to fetch configs: ${response.status}`)

      const configs = await response.json()
      console.log('[MSAL] Received config:', configs)

      msalConfig = {
        auth: {
          clientId: configs['gravitino.authenticator.oauth.azure.client-id'],
          authority: configs['gravitino.authenticator.oauth.azure.authority'],
          redirectUri: configs['gravitino.authenticator.oauth.azure.redirect-uri']
        },
        cache: {
          cacheLocation: 'localStorage',
          storeAuthStateInCookie: false
        }
      }

      msalInstance = new PublicClientApplication(msalConfig)
      await msalInstance.initialize()

      return msalInstance
    } catch (err) {
      configPromise = null // allow retry
      throw err
    }
  })()

  await configPromise

  return msalInstance
}

export function getMsalInstance() {
  if (!msalInstance) {
    throw new Error('[MSAL] Instance not initialized. Call initMsal() first.')
  }

  return msalInstance
}

export function getMsalConfig() {
  if (!msalConfig) {
    console.warn('[MSAL] Config not yet loaded. Call initMsal() first.')
  }

  return msalConfig
}

export async function getAccessToken(scopes = ['user.read']) {
  const instance = getMsalInstance()
  const accounts = instance.getAllAccounts()

  if (accounts.length === 0) {
    throw new Error('No MSAL account found. User might not be logged in.')
  }

  const request = {
    scopes,
    account: accounts[0]
  }

  try {
    // Try silent token acquisition first
    const response = await instance.acquireTokenSilent(request)

    return response.accessToken
  } catch (silentError) {
    // Fallback to interactive login if silent acquisition fails (optional)
    console.warn('Silent token acquisition failed, acquiring token interactively...', silentError)
    try {
      const response = await instance.acquireTokenPopup(request)

      return response.accessToken
    } catch (interactiveError) {
      console.error('Interactive token acquisition failed', interactiveError)
      throw interactiveError
    }
  }
}
