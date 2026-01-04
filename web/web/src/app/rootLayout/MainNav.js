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

import React, { useEffect, useState } from 'react'
import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { ConfigProvider, Menu } from 'antd'
import Icons from '@/components/Icons'
import { ROUTES } from '@/config/routes'
import { usePathname, useSearchParams } from 'next/navigation'

import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { getCurrentEntityOwner } from '@/lib/store/metalakes'
import { to } from '@/lib/utils'

export function MainNav() {
  const [NavItems, setItems] = useState([])
  const router = useRouter()
  const pathname = usePathname()
  const [current, setCurrent] = useState(pathname)
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const auth = useAppSelector(state => state.auth)
  const sys = useAppSelector(state => state.sys)
  const { anthEnable, authUser } = auth
  const dispatch = useAppDispatch()

  useEffect(() => {
    const init = async () => {
      if (pathname === '/metalakes' && !searchParams.get('metalake')) {
        setItems([
          {
            key: ROUTES.Metalakes,
            icon: <Icons.iconify icon='carbon:cloudy' className='my-icon' />,
            label: 'Metalakes'
          }
        ])
      } else {
        const items = [
          {
            key: ROUTES.Catalogs,
            icon: <Icons.iconify icon='carbon:catalog' className='my-icon' />,
            label: 'Catalogs'
          },
          {
            key: ROUTES.Jobs,
            icon: <Icons.iconify icon='carbon:job-run' className='my-icon' />,
            label: 'Jobs'
          },
          {
            key: ROUTES.Compliance,
            icon: <Icons.iconify icon='carbon:ibm-cloud-security-compliance-center' className='my-icon' />,
            label: 'Data Compliance',
            children: [
              {
                key: ROUTES.Tags,
                label: 'Tags'
              },
              {
                key: ROUTES.Policies,
                label: 'Policies'
              },
              {
                key: ROUTES.RowFilters,
                label: 'Row Filters',
                disabled: true
              },
              {
                key: ROUTES.ColumnMasks,
                label: 'Column Masks',
                disabled: true
              }
            ]
          }
        ]

        if (anthEnable && currentMetalake) {
          const ownerData = await to(
            dispatch(
              getCurrentEntityOwner({
                metalake: currentMetalake,
                metadataObjectType: 'metalake',
                metadataObjectFullName: currentMetalake
              })
            )
          )
          if (!authUser || (authUser && authUser.name === ownerData?.name)) {
            items.push({
              key: ROUTES.Access,
              icon: <Icons.iconify icon='la:users-cog' className='my-icon' />,
              label: 'Access',
              children: [
                {
                  key: ROUTES.Users,
                  label: 'Users'
                },
                {
                  key: ROUTES.UserGroups,
                  label: 'User Groups'
                },
                {
                  key: ROUTES.Roles,
                  label: 'Roles'
                }
              ]
            })
          }
        }
        setItems(items)
      }
      if (currentMetalake) {
        if (pathname.startsWith('/catalogs')) {
          setCurrent(ROUTES.Catalogs)
        } else {
          setCurrent(pathname)
        }
      } else {
        setCurrent(ROUTES.Metalakes)
      }
    }
    init()
  }, [pathname, anthEnable, authUser, currentMetalake])

  const onClick = menuItem => {
    setCurrent(menuItem.key)
    if (menuItem.key === ROUTES.Catalogs) {
      router.push(`/catalogs?metalake=${currentMetalake}&catalogType=relational`)
    } else {
      router.push(currentMetalake ? `${menuItem.key}?metalake=${currentMetalake}` : menuItem.key)
    }
  }

  return (
    <div className='flex w-full items-center gap-4'>
      <Link href='/metalakes'>
        <div className='group flex items-center space-x-2'>
          <Icons.gravitino className='size-8 group-hover:text-slate-100) text-white' />
          <span className='hidden text-base font-bold sm:inline-block text-slate-100'>Gravitino</span>
          <span id='gravitino_version' className='hidden font-sans sm:inline-block text-slate-100'>
            {sys.version}
          </span>
        </div>
      </Link>
      <ConfigProvider
        theme={{
          components: {
            Menu: {
              horizontalLineHeight: '4rem',
              iconMarginInlineEnd: 8,
              fontSize: 16,
              darkItemSelectedBg: '#334155',
              darkPopupBg: '#2b3748',
              darkItemColor: '#f1f5f9',
              activeBarHeight: 2
            }
          }
        }}
      >
        <Menu
          theme={'dark'}
          className='h-full min-w-0 flex-auto items-stretch border-0 bg-slate-800'
          onClick={onClick}
          selectedKeys={[current]}
          mode='horizontal'
          items={NavItems}
        />
      </ConfigProvider>
    </div>
  )
}
