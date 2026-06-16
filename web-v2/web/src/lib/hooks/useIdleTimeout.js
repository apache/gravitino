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
 * Sentinel value indicating the hook should be effectively disabled.
 * When idleTimeoutMs is this value, no polling loop or event listeners are attached.
 */
const DISABLED_SENTINEL = Number.MAX_SAFE_INTEGER

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
 * Throttle interval for activity events in milliseconds.
 * Prevents excessive React state updates from high-frequency events like mousemove/scroll.
 */
const ACTIVITY_THROTTLE_MS = 1000

/**
 * Custom hook for idle timeout detection using DOM event listeners and
 * requestAnimationFrame polling. Provides full control over timer reset
 * for "Stay signed in" and cross-tab synchronization.
 *
 * Performance optimizations:
 * - Skips polling loop entirely when idleTimeoutMs is Number.MAX_SAFE_INTEGER (unauthenticated)
 * - Throttles state updates to once per second (only when remaining seconds change)
 * - Throttles activity event handling to avoid excessive resets from high-frequency events
 *
 * @param {Object} options
 * @param {number} [options.idleTimeoutMs] - Idle timeout in milliseconds (default: 15 minutes).
 *   Pass Number.MAX_SAFE_INTEGER to disable the hook entirely.
 * @param {boolean} [options.paused] - When true, DOM activity events are ignored (timer keeps running).
 *   Used to keep the warning modal visible until the user explicitly acts.
 *   Programmatic calls to resetActivity() still work when paused.
 * @returns {{ isIdle: boolean, resetActivity: () => void, idleTimeRemaining: number }}
 */
export function useIdleTimeout({ idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS, paused = false } = {}) {
  const lastActivityRef = useRef(null)
  const rafIdRef = useRef(null)
  const pausedRef = useRef(paused)
  const lastReportedSecondRef = useRef(null)
  const lastResetTimeRef = useRef(0)
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
    lastReportedSecondRef.current = null
  }, [idleTimeoutMs])

  useEffect(() => {
    // Skip entirely when disabled (unauthenticated sentinel)
    if (idleTimeoutMs === DISABLED_SENTINEL) {
      return
    }

    // Initialize the last activity timestamp on mount
    lastActivityRef.current = Date.now()
    lastReportedSecondRef.current = null
    lastResetTimeRef.current = 0

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

      // Throttle activity resets to avoid excessive React state updates
      // from high-frequency events like mousemove and scroll
      const now = Date.now()
      if (now - lastResetTimeRef.current < ACTIVITY_THROTTLE_MS) {
        return
      }
      lastResetTimeRef.current = now

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
    // Throttled to update state only when the remaining seconds change
    const checkIdle = () => {
      const now = Date.now()
      const elapsed = now - lastActivityRef.current
      const remainingMs = Math.max(0, idleTimeoutMs - elapsed)
      const remainingSeconds = Math.ceil(remainingMs / 1000)

      // Only trigger re-render when the displayed second changes
      if (remainingSeconds !== lastReportedSecondRef.current) {
        lastReportedSecondRef.current = remainingSeconds
        setIdleTimeRemaining(remainingMs)
      }

      if (elapsed >= idleTimeoutMs) {
        setIsIdle(true)
      } else {
        rafIdRef.current = requestAnimationFrame(checkIdle)
      }
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
