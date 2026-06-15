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

import { useEffect, useRef, useCallback, useState } from 'react'

/**
 * DOM events that constitute genuine user interaction and reset the idle timer.
 * Events fired while the document is hidden are ignored.
 */
const ACTIVITY_EVENTS = ['mousemove', 'mousedown', 'keydown', 'touchstart', 'scroll', 'visibilitychange']

/**
 * Default idle timeout: 15 minutes in milliseconds.
 * Overridable via NEXT_PUBLIC_IDLE_TIMEOUT_MS environment variable.
 */
const DEFAULT_IDLE_TIMEOUT_MS = (() => {
  const envVal = process.env.NEXT_PUBLIC_IDLE_TIMEOUT_MS
  const parsed = envVal ? Number(envVal) : NaN

  return Number.isFinite(parsed) && parsed > 0 ? parsed : 15 * 60 * 1000
})()

/**
 * Custom hook for idle timeout detection using DOM event listeners and
 * requestAnimationFrame polling. Provides full control over timer reset
 * for "Stay signed in" and cross-tab synchronization.
 *
 * @param {Object} options
 * @param {number} [options.idleTimeoutMs] - Idle timeout in milliseconds (default: 15 minutes)
 * @param {boolean} [options.paused] - When true, DOM activity events are ignored (timer keeps running).
 *   Used to keep the warning modal visible until the user explicitly acts.
 *   Programmatic calls to resetActivity() still work when paused.
 * @returns {{ isIdle: boolean, resetActivity: () => void, idleTimeRemaining: number }}
 */
export function useIdleTimeout({ idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS, paused = false } = {}) {
  const lastActivityRef = useRef(null)
  const rafIdRef = useRef(null)
  const pausedRef = useRef(paused)
  const [isIdle, setIsIdle] = useState(false)
  const [idleTimeRemaining, setIdleTimeRemaining] = useState(idleTimeoutMs)

  // Keep pausedRef in sync with the latest paused prop
  useEffect(() => {
    pausedRef.current = paused
  }, [paused])

  const resetActivity = useCallback(() => {
    lastActivityRef.current = Date.now()
    setIsIdle(false)
    setIdleTimeRemaining(idleTimeoutMs)
  }, [idleTimeoutMs])

  useEffect(() => {
    // Initialize the last activity timestamp on mount
    lastActivityRef.current = Date.now()

    const handleActivityEvent = event => {
      // Ignore events fired while the tab is hidden (IST-REQ-001)
      if (document.hidden) {
        return
      }

      // When paused (e.g. warning modal is shown), ignore DOM activity
      // so the modal stays visible until the user explicitly acts
      if (pausedRef.current) {
        return
      }

      resetActivity()
    }

    // Register activity event listeners
    ACTIVITY_EVENTS.forEach(eventName => {
      if (eventName === 'visibilitychange') {
        document.addEventListener(eventName, handleActivityEvent, { passive: true })
      } else {
        window.addEventListener(eventName, handleActivityEvent, { passive: true })
      }
    })

    // requestAnimationFrame polling loop to check idle state
    const checkIdle = () => {
      const now = Date.now()
      const elapsed = now - lastActivityRef.current
      const remaining = Math.max(0, idleTimeoutMs - elapsed)

      setIdleTimeRemaining(remaining)

      if (elapsed >= idleTimeoutMs) {
        setIsIdle(true)
      }

      rafIdRef.current = requestAnimationFrame(checkIdle)
    }

    rafIdRef.current = requestAnimationFrame(checkIdle)

    return () => {
      // Cleanup event listeners
      ACTIVITY_EVENTS.forEach(eventName => {
        if (eventName === 'visibilitychange') {
          document.removeEventListener(eventName, handleActivityEvent)
        } else {
          window.removeEventListener(eventName, handleActivityEvent)
        }
      })

      // Cancel the rAF loop
      if (rafIdRef.current) {
        cancelAnimationFrame(rafIdRef.current)
      }
    }
  }, [idleTimeoutMs, resetActivity])

  return { isIdle, resetActivity, idleTimeRemaining }
}
