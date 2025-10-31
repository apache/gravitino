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
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import { Button, Dropdown, Flex, Input, Modal, Spin, Table, Tooltip, Typography } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import ConfirmInput from '@/components/ConfirmInput'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import GetOwner from '@/components/GetOwner'
import Loading from '@/components/Loading'
import { useSearchParams } from 'next/navigation'
import dynamic from 'next/dynamic'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { fetchRoles, deleteRole } from '@/lib/store/roles'

const SetOwnerDialog = dynamic(() => import('@/components/SetOwnerDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const CreateRoleDialog = dynamic(() => import('./CreateRoleDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const GrantPrivilegesForRoleDialog = dynamic(() => import('./GrantPrivilegesForRoleDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

export default function RolesPage() {
  const [open, setOpen] = useState(false)
  const [openGrant, setOpenGrant] = useState(false)
  const [editRole, setEditRole] = useState('')
  const [grantOrRevokeRole, setGrantOrRevokeRole] = useState('')
  const [search, setSearch] = useState('')
  const [modal, contextHolder] = Modal.useModal()
  const [openSetOwner, setOpenSetOwner] = useState(false)
  const [ownerRole, setOwnerRole] = useState('')
  const [ownerRefreshKey, setOwnerRefreshKey] = useState(0)
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const store = useAppSelector(state => state.roles)
  const dispatch = useAppDispatch()

  useEffect(() => {
    currentMetalake && dispatch(fetchRoles({ metalake: currentMetalake }))
  }, [dispatch, currentMetalake])

  const tableData = store.rolesData
    ?.filter(r => {
      if (search === '') return true

      return r.includes(search)
    })
    .map(role => {
      return {
        name: role,
        key: role
      }
    })

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleCreateRole = () => {
    setEditRole('')
    setOpen(true)
  }

  const handleUpdateRolePrivileges = role => {
    setEditRole(role)
    setOpen(true)
  }

  const handleSetPrivilege = role => {
    setGrantOrRevokeRole(role)
    setOpenGrant(true)
  }

  const handleSetOwner = role => {
    setOwnerRole(role)
    setOpenSetOwner(true)
  }

  const handleOwnerDialogClose = (open, refresh) => {
    setOpenSetOwner(open)
    if (refresh) setOwnerRefreshKey(k => k + 1)
  }

  const showDeleteConfirm = (NameContext, role) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }

    modal.confirm({
      title: `Are you sure to delete the Role ${role}?`,
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
          await dispatch(deleteRole({ metalake: currentMetalake, role }))
          close()
        }
        confirmFn()
      }
    })
  }

  const columns = useMemo(
    () => [
      {
        title: 'Role Name',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase())
      },
      {
        title: 'Owner',
        dataIndex: 'owner',
        key: 'owner',
        render: (_, record) => (
          <GetOwner
            metalake={currentMetalake}
            metadataObjectType='role'
            metadataObjectFullName={record.name}
            refreshKey={ownerRefreshKey}
          />
        )
      },
      {
        title: 'Actions',
        dataIndex: 'action',
        key: 'action',
        width: 100,
        render: (_, record) => {
          const NameContext = createContext(record.name)

          return (
            <div className='flex gap-2'>
              <NameContext.Provider value={record.name}>{contextHolder}</NameContext.Provider>
              <a>
                <Tooltip title='Edit'>
                  <Icons.Pencil className='size-4' onClick={() => handleUpdateRolePrivileges(record.name)} />
                </Tooltip>
              </a>
              <a>
                <Tooltip title='Delete'>
                  <Icons.Trash2Icon className='size-4' onClick={() => showDeleteConfirm(NameContext, record.name)} />
                </Tooltip>
              </a>
              <Dropdown
                menu={{
                  items: [
                    {
                      label: 'Set Owner',
                      key: 'setOwner'
                    }
                  ],
                  onClick: ({ key }) => {
                    switch (key) {
                      case 'setOwner':
                        handleSetOwner(record.name)
                        break
                    }
                  }
                }}
                trigger={['hover']}
              >
                <Tooltip title='Settings'>
                  <a onClick={e => e.preventDefault()}>
                    <Icons.Settings className='size-4' />
                  </a>
                </Tooltip>
              </Dropdown>
            </div>
          )
        }
      }
    ],
    [currentMetalake, ownerRefreshKey]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <SectionContainer classProps='block'>
      <Title level={2}>Roles</Title>
      <Paragraph type='secondary'>Roles are the entities that can be granted to users or user groups.</Paragraph>
      <Flex justify='flex-end' className='mb-4'>
        <div className='flex w-1/3 gap-4'>
          <Search name='searchCatalogInput' placeholder='Search' onChange={onSearchTable} />
          <Button type='primary' icon={<PlusOutlined />} onClick={handleCreateRole}>
            Create Role
          </Button>
        </div>
      </Flex>
      <Spin spinning={store.rolesLoading}>
        <Table
          style={{ maxHeight: 'calc(100vh - 25rem)' }}
          scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
          dataSource={tableData}
          columns={resizableColumns}
          components={components}
          pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
        />
      </Spin>
      <CreateRoleDialog open={open} setOpen={setOpen} editRole={editRole} metalake={currentMetalake} />
      <GrantPrivilegesForRoleDialog
        open={openGrant}
        setOpen={setOpenGrant}
        role={grantOrRevokeRole}
        metalake={currentMetalake}
      />
      <SetOwnerDialog
        open={openSetOwner}
        setOpen={(v, isRefresh) => handleOwnerDialogClose(v, isRefresh)}
        metalake={currentMetalake}
        metadataObjectType={'role'}
        metadataObjectFullName={ownerRole}
      />
    </SectionContainer>
  )
}
