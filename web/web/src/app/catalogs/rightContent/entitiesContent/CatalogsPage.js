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

import { createContext, useContext, useMemo, useState } from 'react'

import dynamic from 'next/dynamic'
import { useSearchParams } from 'next/navigation'
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import { Button, Dropdown, Flex, Input, Modal, Spin, Table, Tooltip, Typography } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { switchInUseCatalog, deleteCatalog } from '@/lib/store/metalakes'
import { TreeRefContext } from '../../page'
import Tags from '@/components/CustomTags'
import Icons from '@/components/Icons'
import Policies from '@/components/PolicyTag'
import Link from 'next/link'
import Loading from '@/components/Loading'

const CreateCatalogDialog = dynamic(() => import('../CreateCatalogDialog'), {
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

export default function CatalogsPage() {
  const [open, setOpen] = useState(false)
  const [openOwner, setOpenOwner] = useState(false)
  const [editCatalog, setEditCatalog] = useState('')
  const [metadataObjectFullName, setMetadataObjectFullName] = useState('')
  const auth = useAppSelector(state => state.auth)
  const { anthEnable } = auth
  const [modal, contextHolder] = Modal.useModal()
  const [search, setSearch] = useState('')
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake') || ''
  const catalogType = searchParams.get('catalogType') || 'relational'
  const treeRef = useContext(TreeRefContext)
  const store = useAppSelector(state => state.metalakes)
  const dispatch = useAppDispatch()

  const tableData =
    [...store.tableData]
      .sort((a, b) => new Date(b?.audit?.lastModifiedTime).getTime() - new Date(a?.audit?.lastModifiedTime).getTime())
      .filter(c => {
        if (search === '') return c.type === catalogType

        return c.name.includes(search) && c.type === catalogType
      })
      .map(catalog => {
        return {
          ...catalog,
          key: catalog.name,
          tags: [],
          properties: catalog.properties,
          children: undefined
        }
      }) || []

  const providerFitlers = [...store.tableData]
    ?.map(catalog => catalog.provider)
    .filter((value, index, self) => self.indexOf(value) === index)
    .map(p => {
      return {
        text: p,
        value: p
      }
    })

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleCreateCatalog = () => {
    setEditCatalog('')
    setOpen(true)
  }

  const handleEditCatalog = catalog => {
    setEditCatalog(catalog)
    setOpen(true)
  }

  const handleSetOwner = catalog => {
    setMetadataObjectFullName(catalog)
    setOpenOwner(true)
  }

  const showDeleteConfirm = (NameContext, catalog, type) => {
    const inUse = catalog.properties['in-use'] === 'true'
    if (inUse) {
      modal.error({
        title: `Make sure the ${type} ${catalog.name} is not in-use`,
        icon: <ExclamationCircleFilled />,
        okText: 'OK'
      })

      return
    }
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }

    modal.confirm({
      title: `Are you sure to delete the ${type} ${catalog.name}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContext.Consumer>
          {name => (
            <ConfirmInput
              name={name}
              type={type}
              setConfirmInput={setConfirmInput}
              registerValidate={registerValidate}
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
          await dispatch(deleteCatalog({ metalake: currentMetalake, catalog: catalog.name, catalogType: catalog.type }))
          close()
        }
        confirmFn()
      }
    })
  }

  const columns = useMemo(
    () => [
      {
        title: 'Catalog Name',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        width: 200,
        render: (_, record) => (
          <div className='flex items-center gap-0.5'>
            {record.properties?.['in-use'] === 'false' && (
              <Tooltip title='Not In Use'>
                <Icons.BanIcon className='size-3 text-gray-400' />
              </Tooltip>
            )}
            {record.properties?.['in-use'] === 'true' ? (
              <Link
                href={`/catalogs?metalake=${currentMetalake}&catalogType=${catalogType}&catalog=${encodeURIComponent(record.name)}`}
                title={record.name}
                data-refer={`catalog-link-${record.name}`}
                className='hover:text-defaultPrimary'
              >
                {record.name}
              </Link>
            ) : (
              <span>{record.name}</span>
            )}
          </div>
        )
      },
      {
        title: 'Provider',
        dataIndex: 'provider',
        key: 'provider',
        filters: providerFitlers,
        onFilter: (value, record) => record.provider.indexOf(value) === 0,
        ellipsis: true,
        width: 150
      },
      {
        title: 'Comment',
        dataIndex: 'comment',
        key: 'comment',
        ellipsis: true,
        width: 120,
        render: (_, record) => <span>{record.comment || '-'}</span>
      },
      {
        title: 'Tags',
        dataIndex: 'tags',
        key: 'tags',
        ellipsis: true,
        render: (_, record) =>
          record?.node === 'catalog' ? (
            <Tags
              metalake={currentMetalake}
              metadataObjectType={'catalog'}
              metadataObjectFullName={record.name}
              key={`catalog-${record.name}-tags`}
            />
          ) : null
      },
      {
        title: 'Policies',
        dataIndex: 'policies',
        key: 'policies',
        ellipsis: true,
        render: (_, record) =>
          record?.node === 'catalog' ? (
            <Policies
              metalake={currentMetalake}
              metadataObjectType={'catalog'}
              metadataObjectFullName={record.name}
              isLoading={store.tableLoading}
              key={`catalog-${record.name}-policies`}
            />
          ) : null
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
              <a data-refer={`edit-entity-${record.name}`}>
                <Tooltip title='Edit'>
                  <Icons.Pencil className='size-4' onClick={() => handleEditCatalog(record.name)} />
                </Tooltip>
              </a>
              <a data-refer={`delete-entity-${record.name}`}>
                <Tooltip title='Delete'>
                  <Icons.Trash2Icon
                    className='size-4'
                    onClick={() => showDeleteConfirm(NameContext, record, 'catalog')}
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
                  <a data-refer={`settings-catalog-${record.name}`} onClick={e => e.preventDefault()}>
                    <Icons.Settings className='size-4' />
                  </a>
                </Tooltip>
              </Dropdown>
            </div>
          )
        }
      }
    ],
    [currentMetalake, catalogType, anthEnable, store.tableLoading, store.tableData]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  const handleChangeInUse = async (catalogObj, checked) => {
    await dispatch(switchInUseCatalog({ metalake: currentMetalake, catalog: catalogObj.name, isInUse: checked }))
    treeRef.current?.setCatalogInUse(catalogObj.name, checked)
  }

  return (
    <div>
      <Title level={2}>Catalogs</Title>
      <Paragraph type='secondary'>This table lists the data catalogs you have access to.</Paragraph>
      <Flex justify='flex-end' className='mb-4'>
        <div className='flex w-1/2 gap-4'>
          <Search name='searchCatalogInput' placeholder='Search...' value={search} onChange={onSearchTable} />
          <Button type='primary' icon={<PlusOutlined />} onClick={handleCreateCatalog} data-refer='create-catalog-btn'>
            Create Catalog
          </Button>
        </div>
      </Flex>
      <Spin spinning={store.tableLoading}>
        <Table
          data-refer='table-grid'
          style={{ maxHeight: 'calc(100vh - 25rem)' }}
          scroll={{ x: tableWidth, y: 'calc(100vh - 33rem)' }}
          dataSource={tableData}
          columns={resizableColumns}
          components={components}
          pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
        />
        {tableData?.length === 1000 && (
          <span className='float-right text-xs text-slate-300'>up to 1000 data items</span>
        )}
      </Spin>
      {open && (
        <CreateCatalogDialog
          open={open}
          setOpen={setOpen}
          metalake={currentMetalake}
          catalogType={catalogType}
          editCatalog={editCatalog}
          init={true}
        />
      )}
      {openOwner && (
        <SetOwnerDialog
          open={openOwner}
          setOpen={setOpenOwner}
          metalake={currentMetalake}
          metadataObjectType={'catalog'}
          metadataObjectFullName={metadataObjectFullName}
        />
      )}
    </div>
  )
}
