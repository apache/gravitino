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
import { useRouter } from 'next/navigation'
import { Menu } from 'antd'
import { ROUTES } from '@/config/routes'
import { usePathname, useSearchParams } from 'next/navigation'

export default function LeftNav() {
  const [NavItems, setItems] = useState([])
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')

  useEffect(() => {
    if (pathname.startsWith('/compliance')) {
      setItems([
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
      ])
    } else if (pathname.startsWith('/access')) {
      setItems([
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
      ])
    }
  }, [pathname])

  const onClick = menuItem => {
    router.push(`${menuItem.key}?metalake=${currentMetalake}`)
  }

  return (
    <Menu
      mode='inline'
      onClick={onClick}
      selectedKeys={[pathname]}
      style={{ width: 200 }}
      className='h-full'
      items={NavItems}
    />
  )
}
