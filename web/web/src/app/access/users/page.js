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

import { createContext, useMemo, useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import { Button, Flex, Input, Modal, Spin, Table, Tag, Tooltip, Typography } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import ConfirmInput from '@/components/ConfirmInput'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import Loading from '@/components/Loading'
import { useSearchParams } from 'next/navigation'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { fetchUsers, deleteUser } from '@/lib/store/users'

const AddUserDialog = dynamic(() => import('./AddUserDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const GrantRolesForUserDialog = dynamic(() => import('./GrantRolesForUserDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

export default function UsersPage() {
  const [open, setOpen] = useState(false)
  const [openGrant, setOpenGrant] = useState(false)
  const [grantUser, setGrantUser] = useState()
  const [modal, contextHolder] = Modal.useModal()
  const [search, setSearch] = useState('')
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const store = useAppSelector(state => state.users)
  const dispatch = useAppDispatch()

  useEffect(() => {
    currentMetalake && dispatch(fetchUsers({ metalake: currentMetalake, details: true }))
  }, [dispatch, currentMetalake])

  const tableData = store.usersData
    ?.filter(u => {
      if (search === '') return true

      return u?.name.includes(search)
    })
    .map(user => {
      return {
        ...user,
        key: user.name
      }
    })

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleAddUser = () => {
    setOpen(true)
  }

  const handleGrantRoles = user => {
    setGrantUser(user)
    setOpenGrant(true)
  }

  const showDeleteConfirm = (NameContext, user) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }

    modal.confirm({
      title: `Are you sure to delete the User ${user}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContext.Consumer>
          {name => <ConfirmInput name={name} setConfirmInput={setConfirmInput} registerValidate={registerValidate} />}
        </NameContext.Consumer>
      ),
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk(close) {
        if (validateFn && !validateFn()) return

        const confirmFn = async () => {
          await dispatch(deleteUser({ metalake: currentMetalake, user }))
          close()
        }
        confirmFn()
      }
    })
  }

  const columns = useMemo(
    () => [
      {
        title: 'User Name',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        width: 300
      },
      {
        title: 'Roles',
        dataIndex: 'roles',
        key: 'roles',
        ellipsis: true,
        render: roles => (
          <>
            {roles.map(role => {
              return <Tag key={role}>{role}</Tag>
            })}
            {roles.length === 0 && <span>-</span>}
          </>
        )
      },
      {
        title: 'Actions',
        key: 'action',
        width: 100,
        render: (_, record) => {
          const NameContext = createContext(record.name)

          return (
            <div className='flex gap-2'>
              <NameContext.Provider value={record.name}>{contextHolder}</NameContext.Provider>
              <a>
                <Tooltip title='Grant Role'>
                  <Icons.UserRoundCog className='size-4' onClick={() => handleGrantRoles(record)} />
                </Tooltip>
              </a>
              <a>
                <Tooltip title='Delete'>
                  <Icons.Trash2Icon className='size-4' onClick={() => showDeleteConfirm(NameContext, record.name)} />
                </Tooltip>
              </a>
            </div>
          )
        }
      }
    ],
    [currentMetalake]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <SectionContainer classProps='block'>
      <Title level={2}>Users</Title>
      <Paragraph type='secondary'>Users are the entities that can be granted roles.</Paragraph>
      <Flex justify='flex-end' className='mb-4'>
        <div className='flex w-1/3 gap-4'>
          <Search name='searchCatalogInput' placeholder='Search...' onChange={onSearchTable} />
          <Button type='primary' icon={<PlusOutlined />} onClick={handleAddUser}>
            Add User
          </Button>
        </div>
      </Flex>
      <Spin spinning={store.usersLoading}>
        <Table
          style={{ maxHeight: 'calc(100vh - 25rem)' }}
          scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
          dataSource={tableData}
          columns={resizableColumns}
          components={components}
          pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
        />
      </Spin>
      {open && <AddUserDialog open={open} setOpen={setOpen} metalake={currentMetalake} />}
      {openGrant && (
        <GrantRolesForUserDialog open={openGrant} setOpen={setOpenGrant} user={grantUser} metalake={currentMetalake} />
      )}
    </SectionContainer>
  )
}
