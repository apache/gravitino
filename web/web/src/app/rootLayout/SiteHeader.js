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

import React, { useState, useEffect } from 'react'
import { cn } from '@/lib/utils/tailwind'
import { MainNav } from './MainNav'
import UserSetting from './UserSetting'
import { usePathname, useSearchParams, useRouter } from 'next/navigation'
import { Tooltip } from 'antd'
import Icons from '@/components/Icons'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { fetchMetalakes } from '@/lib/store/metalakes'
import GitHubInfo from './GitHubInfo'

export function SiteHeader() {
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const router = useRouter()

  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  useEffect(() => {
    if (pathname && !['/', '/ui', '/login', '/ui/login'].includes(pathname)) {
      dispatch(fetchMetalakes())
    }
  }, [dispatch, pathname])

  // Ensure URL has sensible defaults when missing:
  // - When not on `/metalakes` and `metalake` query is missing/empty/"undefined",
  //   fill it with the first metalake from store.
  // - When on `/catalogs` and `catalogType` query is missing/empty/"undefined",
  //   default to `relational`.
  useEffect(() => {
    try {
      if (store.metalakes.length === 0) {
        return
      }

      const metalakeParam = searchParams.get('metalake')
      const catalogTypeParam = searchParams.get('catalogType')

      // helper to create a new search string preserving other params
      const makeNewSearch = newParams => {
        const params = new URLSearchParams(Array.from(searchParams.entries()))
        Object.keys(newParams).forEach(k => params.set(k, newParams[k]))

        return params.toString()
      }

      // If we're not on the metalakes list page and metalake is missing, fill it
      if (!['/', '/ui', '/metalakes', '/ui/metalakes', '/login', '/ui/login'].includes(pathname) && !metalakeParam) {
        const first = store.metalakes[0].name || ''
        if (first) {
          const qs = makeNewSearch({ metalake: first })
          router.replace(`${pathname}${qs ? `?${qs}` : ''}`)

          return
        }
      }

      // If we're on catalogs route and catalogType is missing, default it
      if ((pathname.startsWith('/catalogs') || pathname.startsWith('/ui/catalogs')) && !catalogTypeParam) {
        const qs = makeNewSearch({ catalogType: 'relational' })
        router.replace(`${pathname}${qs ? `?${qs}` : ''}`)
      }
    } catch (e) {
      // swallow errors silently (non-critical)
    }

    // Intentionally only run when pathname/searchParams/store.metalakes change
  }, [pathname, searchParams?.toString?.(), store.metalakes])

  const handleSystemMode = () => {
    const metalake = currentMetalake || (store.metalakes.length > 0 ? store.metalakes[0].name : '')
    if (pathname === '/metalakes') {
      router.push(`/catalogs?metalake=${metalake}&catalogType=relational`)
    } else {
      router.push(`/metalakes`)
    }
  }

  return (
    <header
      className={cn(
        'sticky top-0 z-40 w-full border-b border-b-[color:theme(colors.borderWhite)] bg-slate-800 text-white',
        {
          hidden: pathname === '/login' || pathname === '/ui/login'
        }
      )}
    >
      <div className='container flex h-16 items-stretch space-x-4 md:justify-between md:space-x-0'>
        <MainNav />
        <div className='flex flex-1 items-center justify-end space-x-4'>
          <nav className='flex items-center space-x-1'>
            <GitHubInfo />
            <div
              className={cn('cursor-pointer rounded p-1.5 hover:bg-slate-700', {
                'bg-gray-100 shadow-inner': pathname === '/metalakes' || pathname === '/ui/metalakes',
                'bg-slate-700': pathname === '/metalakes' || pathname === '/ui/metalakes'
              })}
            >
              <Tooltip title='System Mode'>
                <Icons.Settings onClick={handleSystemMode} className='size-6' />
              </Tooltip>
            </div>
            <UserSetting />
          </nav>
        </div>
      </div>
    </header>
  )
}
