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

import { useEffect, useRef, useCallback } from 'react'

const CHANNEL_NAME = 'grtv-idle-session'
const STORAGE_KEY_PREFIX = 'grtv-idle-broadcast-'

/**
 * Generates a unique tab identifier.
 * Uses crypto.randomUUID() when available, falls back to a random string.
 */
function generateTabId() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID()
  }

  return `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`
}

/**
 * Hook for cross-tab communication using BroadcastChannel with localStorage fallback.
 * Activity messages are throttled to max 1 per 2 seconds (IST-REQ-005).
 *
 * @returns {{ sendMessage: (message: { type: string, timestamp: number }) => void, onMessage: (callback: Function) => void, tabId: string }}
 */
export function useBroadcastChannel() {
  const tabIdRef = useRef(generateTabId())
  const channelRef = useRef(null)
  const callbackRef = useRef(null)
  const lastSendTimeRef = useRef(0)
  const storageListenerRef = useRef(null)

  // Initialize the communication channel
  useEffect(() => {
    const tabId = tabIdRef.current

    if (typeof BroadcastChannel !== 'undefined') {
      // Primary: BroadcastChannel API
      const channel = new BroadcastChannel(CHANNEL_NAME)
      channelRef.current = channel

      channel.onmessage = event => {
        const message = event.data

        // Ignore messages from the same tab
        if (message && message.tabId !== tabId && callbackRef.current) {
          callbackRef.current(message)
        }
      }
    } else {
      // Fallback: localStorage storage events
      const handleStorage = event => {
        if (!event.key || !event.key.startsWith(STORAGE_KEY_PREFIX)) {
          return
        }

        try {
          const message = JSON.parse(event.newValue)

          // Ignore messages from the same tab
          if (message && message.tabId !== tabId && callbackRef.current) {
            callbackRef.current(message)
          }
        } catch {
          // Ignore invalid JSON
        }
      }

      storageListenerRef.current = handleStorage
      window.addEventListener('storage', handleStorage)
    }

    return () => {
      if (channelRef.current) {
        channelRef.current.close()
        channelRef.current = null
      }

      if (storageListenerRef.current) {
        window.removeEventListener('storage', storageListenerRef.current)
        storageListenerRef.current = null
      }
    }
  }, [])

  /**
   * Sends a message to all other tabs.
   * Activity messages are throttled to max 1 per 2 seconds.
   *
   * @param {{ type: string, timestamp: number }} message
   */
  const sendMessage = useCallback(message => {
    const now = Date.now()
    const tabId = tabIdRef.current
    const fullMessage = { ...message, tabId }

    // Throttle activity messages to 1 per 2 seconds
    if (message.type === 'activity') {
      if (now - lastSendTimeRef.current < 2000) {
        return
      }

      lastSendTimeRef.current = now
    }

    if (channelRef.current) {
      channelRef.current.postMessage(fullMessage)
    } else {
      // Fallback: write to localStorage, which fires a storage event in other tabs
      const key = `${STORAGE_KEY_PREFIX}${now}`
      localStorage.setItem(key, JSON.stringify(fullMessage))

      // Clean up after a short delay
      setTimeout(() => {
        localStorage.removeItem(key)
      }, 5000)
    }
  }, [])

  /**
   * Registers a callback to handle incoming messages from other tabs.
   *
   * @param {Function} callback
   */
  const onMessage = useCallback(callback => {
    callbackRef.current = callback
  }, [])

  return { sendMessage, onMessage, tabId: tabIdRef.current }
}
