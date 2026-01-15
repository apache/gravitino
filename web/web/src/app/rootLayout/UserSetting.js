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

import { useEffect, useMemo, useState } from 'react'

import dynamic from 'next/dynamic'
import { LogoutOutlined, PlusOutlined, UserOutlined } from '@ant-design/icons'
import { Avatar, Dropdown, Tooltip } from 'antd'
import { usePathname, useSearchParams, useRouter } from 'next/navigation'
import { cn } from '@/lib/utils/tailwind'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import Loading from '@/components/Loading'
import { fetchMetalakes, resetMetalakeStore } from '@/lib/store/metalakes'
import { logoutAction } from '@/lib/store/auth'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

const CreateMetalakeDialog = dynamic(() => import('@/app/metalakes/CreateMetalakeDialog'), {
  loading: () => <Loading />,
  ssr: false
})

export default function UserSetting() {
  const [openCreateMeta, setOpenCreateMeta] = useState(false)
  const [showLogoutButton, setShowLogoutButton] = useState(false)
  const auth = useAppSelector(state => state.auth)
  const { serviceAdmins, authUser, anthEnable } = auth
  const [session, setSession] = useState({})
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  useEffect(() => {
    if (pathname && !['/', '/ui', '/login', '/ui/login'].includes(pathname)) {
      dispatch(fetchMetalakes())
    }
  }, [dispatch, pathname])

  const handleCreateMetalake = () => {
    setOpenCreateMeta(true)
  }

  const handleLogout = () => {
    dispatch(logoutAction({ router }))
  }

  const items = useMemo(
    () => [
      ...(authUser?.name
        ? [
            {
              key: 'profile',
              label: authUser?.name,
              icon: <UserOutlined style={{ fontSize: 16 }} />,
              className: 'cursor-default pointer-events-none'
            },
            {
              key: 'divider1',
              type: 'divider'
            }
          ]
        : []),
      {
        key: 'metalakes',
        type: 'group',
        label: (
          <div className='flex w-[208px] justify-between'>
            <span>Metalakes</span>
            {[...(serviceAdmins || '').split(','), ...['service_admin']].includes(authUser?.name) && (
              <Tooltip title='Create Metalake'>
                <PlusOutlined className='cursor-pointer text-black' onClick={handleCreateMetalake} />
              </Tooltip>
            )}
          </div>
        ),
        children:
          store.metalakes.map(metalake => ({
            key: metalake.name,
            label: (
              <div
                data-refer={`select-option-${metalake.name}`}
                className='flex w-[200px] justify-between'
                onClick={() => {
                  const isCatalogsPage = pathname.includes('/catalogs')

                  // If on catalogs page, check if metalake is in-use
                  if (isCatalogsPage && metalake.properties['in-use'] !== 'true') {
                    return
                  }

                  // Build new searchParams with updated metalake, preserving other params
                  const params = new URLSearchParams(searchParams.toString())
                  params.set('metalake', metalake.name)

                  // Reset metalake store to avoid data inconsistency
                  dispatch(resetMetalakeStore())

                  // Preserve pathname and only update metalake in query string
                  router.push(`${pathname}?${params.toString()}`)
                }}
              >
                <span
                  className={cn('cursor-pointer text-black', {
                    'text-defaultPrimary': currentMetalake === metalake.name,
                    'opacity-50 cursor-not-allowed': metalake.properties['in-use'] !== 'true'
                  })}
                >
                  {metalake.name}
                </span>
              </div>
            )
          })) || []
      },
      ...(anthEnable
        ? [
            {
              key: 'divider2',
              type: 'divider'
            },
            {
              key: 'logout',
              label: 'Logout',
              icon: <LogoutOutlined style={{ fontSize: 16 }} />,
              onClick: handleLogout
            }
          ]
        : [])
    ],
    [authUser, serviceAdmins, store.metalakes, currentMetalake, anthEnable, searchParams]
  )

  return (
    <>
      <Dropdown
        menu={{ items }}
        overlayClassName='[&_.ant-dropdown-menu-item-group-list]:max-h-[300px] [&_.ant-dropdown-menu-item-group-list]:overflow-y-auto'
      >
        <div role={'button'} tabIndex={0} data-refer='select-metalake'>
          <Avatar
            shape='square'
            icon={<UserOutlined />}
            className='bg-slate-800 hover:bg-slate-700 text-slate-100 !rounded size-8'
          />
        </div>
      </Dropdown>
      {openCreateMeta && <CreateMetalakeDialog open={openCreateMeta} setOpen={setOpenCreateMeta} />}
    </>
  )
}
