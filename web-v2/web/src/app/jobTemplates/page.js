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

import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import dynamic from 'next/dynamic'
import { useRouter, useSearchParams } from 'next/navigation'
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import { Button, Descriptions, Drawer, Flex, Input, Modal, Spin, Table, Tooltip, Typography } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import ConfirmInput from '@/components/ConfirmInput'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import AssociatedTable from '@/components/AssociatedTable'
import { formatToDateTime } from '@/lib/utils'
import Loading from '@/components/Loading'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { fetchJobTemplates, getJobTemplate, deleteJobTemplate } from '@/lib/store/jobs'

const RegisterJobTemplateDialog = dynamic(() => import('../jobs/RegisterJobTemplateDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

export default function JobTemplatesPage() {
  const [open, setOpen] = useState(false)
  const [editJobTemplate, setEditJobTemplate] = useState('')
  const [openDetailJobTemplate, setOpenDetailJobTemplate] = useState(false)
  const [currentJobTemplate, setCurrentJobTemplate] = useState(null)
  const router = useRouter()
  const [modal, contextHolder] = Modal.useModal()
  const [search, setSearch] = useState('')
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake') || ''
  const jobTemplateName = searchParams.get('jobTemplateName') || ''
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.jobs)
  const [isLoadingDetails, setIsLoadingDetails] = useState(false)
  const auth = useAppSelector(state => state.auth)
  const { anthEnable } = auth

  useEffect(() => {
    currentMetalake && dispatch(fetchJobTemplates({ metalake: currentMetalake, details: true }))
  }, [dispatch, currentMetalake])

  const tableData = store.jobTemplates
    ?.filter(t => {
      if (search === '') return true

      return t.name.includes(search)
    })
    .map(jobTemplate => {
      return {
        ...jobTemplate,
        key: jobTemplate.name
      }
    })

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleJobTemplate = () => {
    setEditJobTemplate('')
    setOpen(true)
  }

  const handleEditJobTemplate = async jobTemplate => {
    setEditJobTemplate(jobTemplate)
    setOpen(true)
  }

  const handleViewJobTemplate = async jobTemplate => {
    setIsLoadingDetails(true)
    const { payload: jobTemplateDetail } = await dispatch(getJobTemplate({ metalake: currentMetalake, jobTemplate }))
    setCurrentJobTemplate(jobTemplateDetail)
    setOpenDetailJobTemplate(true)
    setIsLoadingDetails(false)
  }

  const onClose = () => {
    setOpenDetailJobTemplate(false)
    setCurrentJobTemplate(null)
  }

  useEffect(() => {
    if (jobTemplateName) {
      handleViewJobTemplate(jobTemplateName)
    }
  }, [jobTemplateName])

  const showDeleteConfirm = (NameContext, jobTemplate, type) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }

    modal.confirm({
      title: `Are you sure to cancel the Job Template ${jobTemplate}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContext.Consumer>
          {name => (
            <ConfirmInput
              name={name}
              type={type}
              confirmTips={`Please enter \"${jobTemplate}\" to confirm deletion!`}
              notMatchTips={`The entered name does not match the job template name`}
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
          await dispatch(deleteJobTemplate({ metalake: currentMetalake, jobTemplate }))
          close()
        }
        confirmFn()
      }
    })
  }

  const columns = useMemo(
    () => [
      {
        title: 'Template Name',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        width: 300
      },
      {
        title: 'Creator',
        dataIndex: ['audit', 'creator'],
        key: 'creator',
        ellipsis: true,
        width: 200
      },
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
        title: 'Comment',
        dataIndex: 'comment',
        key: 'comment',
        ellipsis: true
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
                  <Icons.Pencil className='size-4' onClick={() => handleEditJobTemplate(record.name)} />
                </Tooltip>
              </a>
              <a>
                <Tooltip title='View Details'>
                  <Icons.Eye className='size-4' onClick={() => handleViewJobTemplate(record.name)} />
                </Tooltip>
              </a>
              <a>
                <Tooltip title='Delete'>
                  <Icons.Trash2Icon
                    className='size-4'
                    onClick={() => showDeleteConfirm(NameContext, record.name, 'jobTemplate')}
                  />
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
      <div className='h-full bg-white p-6'>
        <Title level={2} className='flex items-center gap-1'>
          <Icons.Undo2 className='size-6 cursor-pointer' onClick={() => router.back()} />
          Job Templates
        </Title>
        <Paragraph type='secondary'>This table lists the job templates you have access to.</Paragraph>
        <Flex justify='flex-end' className='mb-4'>
          <div className='flex w-1/3 gap-4'>
            <Search name='searchCatalogInput' placeholder='Search...' onChange={onSearchTable} />
            <Button type='primary' icon={<PlusOutlined />} onClick={handleJobTemplate}>
              Register Job Template
            </Button>
          </div>
        </Flex>
        <Spin spinning={store.isJobTemplatesLoading}>
          <Table
            style={{ maxHeight: 'calc(100vh - 25rem)' }}
            scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
            dataSource={tableData}
            columns={resizableColumns}
            components={components}
            pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
          />
        </Spin>
        {openDetailJobTemplate && (
          <Drawer
            title={`View Details - ${currentJobTemplate?.name || ''}`}
            loading={isLoadingDetails}
            onClose={onClose}
            open={openDetailJobTemplate}
            width={'40%'}
          >
            <Title level={5} className='mb-2'>
              Basic Information
            </Title>
            <Descriptions column={1} bordered size='small'>
              <Descriptions.Item label='Template Name'>{currentJobTemplate?.name}</Descriptions.Item>
              <Descriptions.Item label='Job Type'>{currentJobTemplate?.jobType}</Descriptions.Item>
              <Descriptions.Item label='Executable'>{currentJobTemplate?.executable}</Descriptions.Item>
              <Descriptions.Item label='Comment'>{currentJobTemplate?.comment || '-'}</Descriptions.Item>
              {currentJobTemplate?.jobType === 'spark' && (
                <Descriptions.Item label='Class Name'>{currentJobTemplate?.className || '-'}</Descriptions.Item>
              )}
              <Descriptions.Item label='Arguments'>
                {currentJobTemplate?.arguments?.length === 1
                  ? currentJobTemplate?.arguments[0]
                  : currentJobTemplate?.arguments?.length
                    ? currentJobTemplate?.arguments.join(', ')
                    : '-'}
              </Descriptions.Item>
              {currentJobTemplate?.jobType === 'shell' && (
                <Descriptions.Item label='Script(s)'>
                  {currentJobTemplate?.scripts && currentJobTemplate.scripts.length > 0
                    ? currentJobTemplate.scripts.map((script, index) => (
                        <span key={index} className='mb-2 block break-words last:mb-0'>
                          {script}
                        </span>
                      ))
                    : '-'}
                </Descriptions.Item>
              )}
            </Descriptions>
            <Title level={5} className='mt-4 mb-2'>
              Environment Variable(s)
            </Title>
            <Table
              size='small'
              pagination={false}
              rowKey='key'
              dataSource={
                currentJobTemplate?.environments && Object.keys(currentJobTemplate.environments).length > 0
                  ? Object.entries(currentJobTemplate.environments).map(([key, value]) => ({
                      key,
                      value
                    }))
                  : []
              }
              columns={[
                {
                  title: 'Env Var Name',
                  dataIndex: 'key',
                  key: 'key',
                  ellipsis: true
                },
                {
                  title: 'Env Var Value',
                  dataIndex: 'value',
                  key: 'value',
                  ellipsis: true,
                  render: value => value || '-'
                }
              ]}
            />
            <Title level={5} className='mt-4 mb-2'>
              Custom Fields
            </Title>
            <Table
              size='small'
              pagination={false}
              rowKey='key'
              dataSource={
                currentJobTemplate?.customFields && Object.keys(currentJobTemplate.customFields).length > 0
                  ? Object.entries(currentJobTemplate.customFields).map(([key, value]) => ({
                      key,
                      value
                    }))
                  : []
              }
              columns={[
                {
                  title: 'Key',
                  dataIndex: 'key',
                  key: 'key',
                  ellipsis: true
                },
                {
                  title: 'Value',
                  dataIndex: 'value',
                  key: 'value',
                  ellipsis: true,
                  render: value => value || '-'
                }
              ]}
            />
            {currentJobTemplate?.jobType === 'spark' && (
              <>
                <Title level={5} className='mt-4 mb-2'>
                  Jars
                </Title>
                <Table
                  size='small'
                  pagination={false}
                  rowKey='value'
                  dataSource={
                    currentJobTemplate?.jars && currentJobTemplate.jars.length > 0
                      ? currentJobTemplate.jars.map(value => ({ value }))
                      : []
                  }
                  columns={[
                    {
                      title: 'Jar',
                      dataIndex: 'value',
                      key: 'value',
                      ellipsis: true
                    }
                  ]}
                />
                <Title level={5} className='mt-4 mb-2'>
                  Files
                </Title>
                <Table
                  size='small'
                  pagination={false}
                  rowKey='value'
                  dataSource={
                    currentJobTemplate?.files && currentJobTemplate.files.length > 0
                      ? currentJobTemplate.files.map(value => ({ value }))
                      : []
                  }
                  columns={[
                    {
                      title: 'File',
                      dataIndex: 'value',
                      key: 'value',
                      ellipsis: true
                    }
                  ]}
                />
                <Title level={5} className='mt-4 mb-2'>
                  Archives
                </Title>
                <Table
                  size='small'
                  pagination={false}
                  rowKey='value'
                  dataSource={
                    currentJobTemplate?.archives && currentJobTemplate.archives.length > 0
                      ? currentJobTemplate.archives.map(value => ({ value }))
                      : []
                  }
                  columns={[
                    {
                      title: 'Archive',
                      dataIndex: 'value',
                      key: 'value',
                      ellipsis: true
                    }
                  ]}
                />
                <Title level={5} className='mt-4 mb-2'>
                  Config(s)
                </Title>
                <Table
                  size='small'
                  pagination={false}
                  rowKey='key'
                  dataSource={
                    currentJobTemplate?.configs && Object.keys(currentJobTemplate.configs).length > 0
                      ? Object.entries(currentJobTemplate.configs).map(([key, value]) => ({
                          key,
                          value
                        }))
                      : []
                  }
                  columns={[
                    {
                      title: 'Config Name',
                      dataIndex: 'key',
                      key: 'key',
                      ellipsis: true
                    },
                    {
                      title: 'Config Value',
                      dataIndex: 'value',
                      key: 'value',
                      ellipsis: true,
                      render: value => value || '-'
                    }
                  ]}
                />
              </>
            )}
            <div className='my-4'>
              <Typography.Title level={5} className='mb-2'>
                Details
              </Typography.Title>
              <Descriptions size='small' bordered column={1} labelStyle={{ width: 180 }}>
                <Descriptions.Item label='Creator'>{currentJobTemplate?.audit?.creator || '-'}</Descriptions.Item>
                <Descriptions.Item label='Created At'>
                  {currentJobTemplate?.audit?.createTime ? formatToDateTime(currentJobTemplate.audit.createTime) : '-'}
                </Descriptions.Item>
                <Descriptions.Item label='Updated At'>
                  {currentJobTemplate?.audit?.lastModifiedTime
                    ? formatToDateTime(currentJobTemplate.audit.lastModifiedTime)
                    : '-'}
                </Descriptions.Item>
              </Descriptions>
            </div>
            {anthEnable && (
              <>
                <Title level={5} className='mt-4 mb-2'>
                  Associated Roles
                </Title>
                <AssociatedTable
                  metalake={currentMetalake}
                  metadataObjectType={'job_template'}
                  metadataObjectFullName={currentJobTemplate?.name}
                />
              </>
            )}
          </Drawer>
        )}
        {open && (
          <RegisterJobTemplateDialog
            open={open}
            setOpen={setOpen}
            metalake={currentMetalake}
            editJobTemplate={editJobTemplate}
            details={true}
          />
        )}
      </div>
    </SectionContainer>
  )
}
