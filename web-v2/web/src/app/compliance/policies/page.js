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
import { useRouter, useSearchParams } from 'next/navigation'
import { ExclamationCircleFilled, PlusOutlined, StopOutlined } from '@ant-design/icons'
import {
  Button,
  Drawer,
  Dropdown,
  Empty,
  Flex,
  Input,
  Modal,
  Space,
  Spin,
  Switch,
  Table,
  Tag,
  Tooltip,
  Typography,
  theme as antdTheme
} from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import ConfirmInput from '@/components/ConfirmInput'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import CreatePolicyDialog from './CreatePolicyDialog'
import { formatToDateTime } from '@/lib/utils'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { use } from 'react'
import { fetchPolicies, deletePolicy, getPolicyDetails, enableOrDisablePolicy } from '@/lib/store/policies'

const { Title, Paragraph } = Typography
const { Search } = Input

export default function PoliciesPage() {
  const [open, setOpen] = useState(false)
  const [editPolicy, setEditPolicy] = useState('')
  const [search, setSearch] = useState('')
  const [modal, contextHolder] = Modal.useModal()
  const router = useRouter()
  const [currentPolicy, setCurrentPolicy] = useState(null)
  const [openPolicy, setOpenPolicy] = useState(false)
  const { token } = antdTheme.useToken()
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.policies)
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const [detailsLoading, setDetailsLoading] = useState(false)

  useEffect(() => {
    currentMetalake && dispatch(fetchPolicies({ metalake: currentMetalake, details: true }))
  }, [dispatch, currentMetalake])

  const tableData = store.policiesData
    ?.filter(p => {
      if (search === '') return true

      return p?.name.includes(search)
    })
    .map(policy => {
      return {
        ...policy,
        createTime: policy.audit.createTime,
        key: policy.name
      }
    })

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleCreatePolicy = () => {
    setEditPolicy('')
    setOpen(true)
  }

  const handleEditPolicy = policy => {
    setEditPolicy(policy)
    setOpen(true)
  }

  const showDeleteConfirm = (NameContext, policy) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }
    modal.confirm({
      title: `Are you sure to delete the Policy ${policy}?`,
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
          await dispatch(deletePolicy({ metalake: currentMetalake, policy }))
          close()
        }
        confirmFn()
      }
    })
  }

  const handleClick = name => () => {
    router.push(`/metadataObjectsForPolicy?policy=${name}&metalake=${currentMetalake}`)
  }

  const handleViewPolicy = async policy => {
    setDetailsLoading(true)
    const { payload: policyDetail } = await dispatch(getPolicyDetails({ metalake: currentMetalake, policy }))
    setDetailsLoading(false)
    setCurrentPolicy(policyDetail)
    setOpenPolicy(true)
  }

  const onClose = () => {
    setOpenPolicy(false)
  }

  const handleEnableOrDisable = async (policy, enabled) => {
    await dispatch(enableOrDisablePolicy({ metalake: currentMetalake, policy, data: { enable: !enabled } }))
  }

  const columns = useMemo(
    () => [
      {
        title: 'Policy Name',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        width: 200,
        render: (_, record) => (
          <Tag
            key={record.name}
            icon={!record.enabled ? <StopOutlined style={{ color: 'white' }} /> : null}
            closable={false}
            onClick={handleClick(record.name)}
            color={record.enabled ? token.colorPrimary : token.colorTextDisabled}
            style={{ userSelect: 'none' }}
            className='cursor-pointer'
          >
            <span>{record.name.length > 20 ? `${record.name.slice(0, 20)}...` : record.name}</span>
          </Tag>
        )
      },
      {
        title: 'Policy Type',
        dataIndex: 'policyType',
        key: 'policyType',
        ellipsis: true,
        width: 150,
        render: (_, record) => <>{record.policyType}</>
      },
      {
        title: 'Comment',
        dataIndex: 'comment',
        key: 'comment',
        ellipsis: true,
        width: 300,
        render: comment => <span>{comment || '-'}</span>
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
                <Tooltip title='Edit'>
                  <Icons.Pencil className='size-4' onClick={() => handleEditPolicy(record.name)} />
                </Tooltip>
              </a>
              <a>
                <Tooltip title='View Details'>
                  <Icons.Eye className='size-4' onClick={() => handleViewPolicy(record.name)} />
                </Tooltip>
              </a>
              <Dropdown
                menu={{
                  items: [
                    { label: 'Delete', key: 'delete' },
                    {
                      label: record.enabled ? 'Disable' : 'Enable',
                      key: 'enableOrDisable'
                    }
                  ],
                  onClick: ({ key }) => {
                    switch (key) {
                      case 'delete':
                        showDeleteConfirm(NameContext, record.name)
                        break
                      case 'enableOrDisable':
                        handleEnableOrDisable(record.name, record.enabled)
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
    [currentMetalake, store.policiesData]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <SectionContainer classProps='block'>
      <Title level={2}>Policies</Title>
      <Paragraph type='secondary'>This table lists the policies you have access to.</Paragraph>
      <Flex justify='flex-end' className='mb-4'>
        <div className='flex w-1/3 gap-4'>
          <Search name='searchCatalogInput' placeholder='Search...' onChange={onSearchTable} />
          <Button type='primary' icon={<PlusOutlined />} onClick={handleCreatePolicy}>
            Create Policy
          </Button>
        </div>
      </Flex>
      <Spin spinning={store.policiesLoading}>
        <Table
          style={{ maxHeight: 'calc(100vh - 25rem)' }}
          scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
          dataSource={tableData}
          columns={resizableColumns}
          components={components}
          pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
        />
      </Spin>
      {openPolicy && (
        <Drawer
          title={`View ${currentPolicy?.name} details`}
          loading={detailsLoading}
          onClose={onClose}
          open={openPolicy}
        >
          <>
            <div className='my-4'>
              <div className='text-sm text-slate-400'>Policy Name</div>
              <span className='break-words text-base'>{currentPolicy?.name}</span>
            </div>
            <div className='my-4'>
              <div className='text-sm text-slate-400'>Enabled</div>
              <span className='break-words text-base'>
                <Switch checked={currentPolicy?.enabled} disabled size='small' />
              </span>
            </div>
            <div className='my-4'>
              <div className='text-sm text-slate-400'>Policy Type</div>
              <span className='break-words text-base'>{currentPolicy?.policyType}</span>
            </div>
            <div className='my-4'>
              <div className='text-sm text-slate-400'>Supported Object Types</div>
              <span className='break-words text-base'>
                {currentPolicy?.content?.supportedObjectTypes.length === 1
                  ? currentPolicy?.content?.supportedObjectTypes[0]
                  : currentPolicy?.content?.supportedObjectTypes.length
                    ? currentPolicy?.content?.supportedObjectTypes.join(', ')
                    : '-'}
              </span>
            </div>
            <div className='my-4'>
              <div className='mb-1 text-sm text-slate-400'>Rule(s)</div>
              <Space.Compact className='max-h-80 w-full overflow-auto'>
                <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                  <span className='bg-gray-100 p-1'>Rule Name</span>
                  {currentPolicy?.content?.customRules
                    ? Object.keys(currentPolicy?.content?.customRules).map(rule => (
                        <span key={rule} className='truncate p-1' title={rule}>
                          {rule}
                        </span>
                      ))
                    : null}
                </Space.Compact>
                <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                  <span className='bg-gray-100 p-1'>Rule Content</span>
                  {currentPolicy?.content?.customRules
                    ? Object.values(currentPolicy?.content?.customRules).map(ruleContent => (
                        <span key={ruleContent} className='truncate p-1' title={ruleContent}>
                          {ruleContent || '-'}
                        </span>
                      ))
                    : null}
                </Space.Compact>
              </Space.Compact>
            </div>
            <div className='my-4'>
              <div className='text-sm text-slate-400'>Comment</div>
              <span className='break-words text-base'>{currentPolicy?.comment || '-'}</span>
            </div>
            <div className='my-4'>
              <div className='mb-1 text-sm text-slate-400'>Properties</div>
              {currentPolicy?.content?.properties && Object.keys(currentPolicy?.content?.properties).length > 0 ? (
                <Space.Compact className='max-h-80 w-full overflow-auto'>
                  <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                    <span className='bg-gray-100 p-1'>Key</span>
                    {currentPolicy?.content?.properties
                      ? Object.keys(currentPolicy?.content?.properties).map(key => (
                          <span key={key} className='truncate p-1' title={key}>
                            {key}
                          </span>
                        ))
                      : null}
                  </Space.Compact>
                  <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                    <span className='bg-gray-100 p-1'>Value</span>
                    {currentPolicy?.content?.properties
                      ? Object.values(currentPolicy?.content?.properties).map(value => (
                          <span key={value} className='truncate p-1' title={value}>
                            {value || '-'}
                          </span>
                        ))
                      : null}
                  </Space.Compact>
                </Space.Compact>
              ) : (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />
              )}
            </div>
          </>
        </Drawer>
      )}
      <CreatePolicyDialog open={open} setOpen={setOpen} metalake={currentMetalake} editPolicy={editPolicy} />
    </SectionContainer>
  )
}
