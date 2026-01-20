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

import { useCallback, useState, useMemo, useEffect, createContext } from 'react'
import dynamic from 'next/dynamic'
import Loading from '@/components/Loading'
import SectionContainer from '@/components/SectionContainer'
import { useRouter } from 'next/navigation'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { Button, Dropdown, Flex, Input, Modal, Popover, Space, Spin, Table, Tooltip, Typography } from 'antd'
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import { useAntdColumnResize } from 'react-antd-column-resize'
import {
  fetchMetalakes,
  deleteMetalake,
  resetTree,
  switchMetalakeInUse,
  resetMetalakeStore
} from '@/lib/store/metalakes'
import { to } from '@/lib/utils'
import { formatToDateTime } from '@/lib/utils/date'
import Icons from '@/components/Icons'
import GetOwner from '@/components/GetOwner'
import PropertiesContent from '@/components/PropertiesContent'

const CreateMetalakeDialog = dynamic(() => import('./CreateMetalakeDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const SetOwnerDialog = dynamic(() => import('@/components/SetOwnerDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const ConfirmInput = dynamic(() => import('@/components/ConfirmInput'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

const MetalakeList = () => {
  const [open, setOpen] = useState(false)
  const [openOwner, setOpenOwner] = useState(false)
  const [editMetalake, setEditMetalake] = useState('')
  const [metadataObjectFullName, setMetadataObjectFullName] = useState('')
  const router = useRouter()
  const [modal, contextHolder] = Modal.useModal()
  const [search, setSearch] = useState('')
  const [ownerRefreshKey, setOwnerRefreshKey] = useState(0)
  const auth = useAppSelector(state => state.auth)
  const { serviceAdmins, authUser, anthEnable } = auth
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)
  const [tableData, setTableData] = useState([])

  useEffect(() => {
    dispatch(resetMetalakeStore())
    dispatch(fetchMetalakes())
  }, [dispatch])

  useEffect(() => {
    const filteredData = store.metalakes
      .filter(i => i.name.toLowerCase().includes(search.toLowerCase()))
      .map(i => ({ ...i, key: i.name }))

    setTableData(filteredData)
  }, [dispatch, store.metalakes, search])

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleCreateMetalake = () => {
    setEditMetalake('')
    setOpen(true)
  }

  const handleEditMetalake = metalake => {
    setEditMetalake(metalake)
    setOpen(true)
  }

  const handleToCatalogsPage = metalake => {
    dispatch(resetTree())
    router.push(`/catalogs?metalake=${metalake}&catalogType=relational`)
  }

  const handleSetOwner = metalake => {
    setMetadataObjectFullName(metalake)
    setOpenOwner(true)
  }

  const handleOwnerDialogClose = (open, refresh) => {
    setOpenOwner(open)
    if (refresh) setOwnerRefreshKey(k => k + 1)
  }

  const propertyContent = properties => <PropertiesContent properties={properties} />

  const showDeleteConfirm = (NameContext, metalake, type, isInUse = false) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }

    modal.confirm({
      title: `Are you sure to delete the ${type} ${name}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContext.Consumer>
          {name => (
            <ConfirmInput
              name={name}
              type={type}
              setConfirmInput={setConfirmInput}
              registerValidate={registerValidate}
              isInUse={isInUse}
            />
          )}
        </NameContext.Consumer>
      ),
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk(close) {
        if (validateFn && !validateFn()) return

        const confirmFn = async () => {
          const [err, res] = await to(dispatch(deleteMetalake(metalake)))
          if (err || !res) {
            throw new Error(err)
          }
          close()
        }
        confirmFn()
      }
    })
  }

  const handleChangeInUse = async (metalakeObj, checked) => {
    const [err, res] = await to(dispatch(switchMetalakeInUse({ name: metalakeObj.name, isInUse: checked })))
    if (err || !res) {
      throw new Error(err)
    }
  }

  const columns = useMemo(
    () => [
      {
        title: 'Metalake Name',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        width: 300,
        render: (_, record) => (
          <div className='flex items-center gap-0.5'>
            {record.properties?.['in-use'] === 'false' && (
              <Tooltip title='Not In-Use'>
                <Icons.BanIcon className='size-3 text-gray-400' />
              </Tooltip>
            )}
            {record.properties?.['in-use'] === 'true' ? (
              <a
                data-refer={`metalake-link-${record.name}`}
                className='hover:text-defaultPrimary'
                onClick={() => handleToCatalogsPage(record.name)}
              >
                {record.name}
              </a>
            ) : (
              <span>{record.name}</span>
            )}
          </div>
        )
      },
      {
        title: 'Creator',
        dataIndex: ['audit', 'creator'],
        key: 'creator',
        ellipsis: true
      },
      ...(anthEnable
        ? [
            {
              title: 'Owner',
              key: 'owner',
              width: 150,
              render: (_, record) => (
                <GetOwner
                  metalake={record.name}
                  metadataObjectType='metalake'
                  metadataObjectFullName={record.name}
                  refreshKey={ownerRefreshKey}
                />
              )
            }
          ]
        : []),
      {
        title: 'Created At',
        dataIndex: ['audit', 'createTime'],
        sorter: (a, b) => new Date(a.audit.createTime).getTime() - new Date(b.audit.createTime).getTime(),
        key: 'createTime',
        ellipsis: true,
        width: 200,
        render: (_, record) => <>{formatToDateTime(record.audit.createTime)}</>
      },
      {
        title: 'Properties',
        dataIndex: 'properties',
        key: 'properties',
        ellipsis: true,
        width: 100,
        render: (_, record) => (
          <>
            {record.properties && Object.keys(record.properties).length > 0 ? (
              <Popover
                key={record.key}
                placement='bottom'
                title={<span>Properties</span>}
                content={propertyContent(record.properties)}
              >
                <a className='text-defaultPrimary'>{Object.keys(record.properties)?.length}</a>
              </Popover>
            ) : (
              <span>0</span>
            )}
          </>
        )
      },
      {
        title: 'Comment',
        dataIndex: 'comment',
        key: 'comment',
        ellipsis: true
      },
      {
        title: 'Actions',
        key: 'action',
        width: 150,
        render: (_, record) => {
          const NameContext = createContext(record.name)

          return (
            <div className='flex gap-2' key={record.name}>
              <NameContext.Provider value={record.name}>{contextHolder}</NameContext.Provider>
              {([...(serviceAdmins || '').split(',')].includes(authUser?.name) || !authUser) && (
                <a data-refer={`edit-metalake-${record.name}`}>
                  <Tooltip title='Edit'>
                    <Icons.Pencil className='size-4' onClick={() => handleEditMetalake(record.name)} />
                  </Tooltip>
                </a>
              )}
              <a data-refer={`delete-metalake-${record.name}`}>
                <Tooltip title='Delete'>
                  <Icons.Trash2Icon
                    className='size-4'
                    onClick={() =>
                      showDeleteConfirm(NameContext, record.name, 'metalake', record.properties?.['in-use'] === 'true')
                    }
                  />
                </Tooltip>
              </a>
              <Dropdown
                menu={{
                  items: anthEnable
                    ? [
                        {
                          label: 'Set Owner',
                          key: 'setOwner'
                        },
                        {
                          label: record.properties?.['in-use'] === 'true' ? 'Not In-Use' : 'In-Use',
                          key: record.properties?.['in-use'] === 'true' ? 'notInUse' : 'inUse'
                        }
                      ]
                    : [
                        {
                          label: record.properties?.['in-use'] === 'true' ? 'Not In-Use' : 'In-Use',
                          key: record.properties?.['in-use'] === 'true' ? 'notInUse' : 'inUse'
                        }
                      ],
                  onClick: ({ key }) => {
                    switch (key) {
                      case 'setOwner':
                        handleSetOwner(record.name)
                        break
                      case 'inUse':
                        handleChangeInUse(record, true)
                        break
                      case 'notInUse':
                        handleChangeInUse(record, false)
                        break
                    }
                  }
                }}
                trigger={['hover']}
              >
                <Tooltip title='Settings'>
                  <a data-refer={`settings-metalake-${record.name}`} onClick={e => e.preventDefault()}>
                    <Icons.Settings className='size-4' />
                  </a>
                </Tooltip>
              </Dropdown>
            </div>
          )
        }
      }
    ],
    [anthEnable, ownerRefreshKey]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <SectionContainer classProps='block'>
      <div className='h-full bg-white p-6'>
        <Title level={2} data-refer='metalake-page-title'>
          Metalakes
        </Title>
        <Paragraph type='secondary'>
          A metalake is the top-level container for data in Gravitino. Within a metalake, Gravitino provides a 3-level
          namespace for organizing data: catalog, schemas/databases, and tables/views.
        </Paragraph>
        <Flex justify='flex-end' className='mb-4'>
          <div className='flex w-1/3 gap-4'>
            <Search
              data-refer='query-metalake'
              name='searchCatalogInput'
              placeholder='Search...'
              onChange={onSearchTable}
            />
            {([...(serviceAdmins || '').split(',')].includes(authUser?.name) || !anthEnable) && (
              <Button
                data-refer='create-metalake-btn'
                type='primary'
                icon={<PlusOutlined />}
                onClick={handleCreateMetalake}
              >
                Create Metalake
              </Button>
            )}
          </div>
        </Flex>
        <Table
          data-refer='metalake-table-grid'
          style={{ maxHeight: 'calc(100vh - 25rem)' }}
          scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
          dataSource={tableData}
          columns={resizableColumns}
          components={components}
          pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
        />
        {open && <CreateMetalakeDialog open={open} setOpen={setOpen} editMetalake={editMetalake} />}
        {openOwner && (
          <SetOwnerDialog
            open={openOwner}
            setOpen={(v, isRefresh) => handleOwnerDialogClose(v, isRefresh)}
            metalake={metadataObjectFullName}
            metadataObjectType={'metalake'}
            metadataObjectFullName={metadataObjectFullName}
          />
        )}
      </div>
    </SectionContainer>
  )
}

export default MetalakeList
