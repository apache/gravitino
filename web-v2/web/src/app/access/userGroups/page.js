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
import { fetchUserGroups, deleteUserGroup } from '@/lib/store/userGroups'

const AddUserGroupDialog = dynamic(() => import('./AddUserGroupDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const GrantRolesForUserGroupDialog = dynamic(() => import('./GrantRolesForUserGroupDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

export default function UserGroupsPage() {
  const [open, setOpen] = useState(false)
  const [openGrant, setOpenGrant] = useState(false)
  const [grantUserGroup, setGrantUserGroup] = useState()
  const [modal, contextHolder] = Modal.useModal()
  const [search, setSearch] = useState('')
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const store = useAppSelector(state => state.userGroups)
  const dispatch = useAppDispatch()

  useEffect(() => {
    currentMetalake && dispatch(fetchUserGroups({ metalake: currentMetalake, details: true }))
  }, [dispatch, currentMetalake])

  const tableData = store.userGroupsData
    ?.filter(g => {
      if (search === '') return true

      return g?.name.includes(search)
    })
    .map(group => {
      return {
        ...group,
        key: group.name
      }
    })

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleAddUser = () => {
    setOpen(true)
  }

  const handleGrantRoles = userGroup => {
    setGrantUserGroup(userGroup)
    setOpenGrant(true)
  }

  const showDeleteConfirm = (NameContex, group) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }
    modal.confirm({
      title: `Are you sure to delete the User Group ${group}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContex.Consumer>
          {name => <ConfirmInput name={name} setConfirmInput={setConfirmInput} registerValidate={registerValidate} />}
        </NameContex.Consumer>
      ),
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk(close) {
        if (validateFn && !validateFn()) return

        const confirmFn = async () => {
          await dispatch(deleteUserGroup({ metalake: currentMetalake, group }))
          close()
        }
        confirmFn()
      }
    })
  }

  const columns = useMemo(
    () => [
      {
        title: 'Group Name',
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
      <Title level={2}>User Groups</Title>
      <Paragraph type='secondary'>User groups are the entities that can be granted roles.</Paragraph>
      <Flex justify='flex-end' className='mb-4'>
        <div className='flex w-1/3 gap-4'>
          <Search name='searchCatalogInput' placeholder='Search...' onChange={onSearchTable} />
          <Button type='primary' icon={<PlusOutlined />} onClick={handleAddUser}>
            Add User Group
          </Button>
        </div>
      </Flex>
      <Spin spinning={store.userGroupsLoading}>
        <Table
          style={{ maxHeight: 'calc(100vh - 25rem)' }}
          scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
          dataSource={tableData}
          columns={resizableColumns}
          components={components}
          pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
        />
      </Spin>
      {open && <AddUserGroupDialog open={open} setOpen={setOpen} metalake={currentMetalake} />}
      {openGrant && (
        <GrantRolesForUserGroupDialog
          open={openGrant}
          setOpen={setOpenGrant}
          userGroup={grantUserGroup}
          metalake={currentMetalake}
        />
      )}
    </SectionContainer>
  )
}
