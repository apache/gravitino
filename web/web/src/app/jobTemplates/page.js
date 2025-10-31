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
import { Button, Drawer, Flex, Input, Modal, Space, Spin, Table, Tooltip, Typography } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import ConfirmInput from '@/components/ConfirmInput'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
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
      title: `Are you sure to cancel the Job Template ${jobTemplateName}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContext.Consumer>
          {name => (
            <ConfirmInput
              name={name}
              type={type}
              confirmTips={`Please enter \"${jobTemplateName}\" to confirm deletion!`}
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
              Create a new job template
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
          >
            <>
              <div className='my-4'>
                <div className='text-sm text-slate-400'>Template Name</div>
                <span className='break-words text-base'>{currentJobTemplate?.name}</span>
              </div>
              <div className='my-4'>
                <div className='text-sm text-slate-400'>Job Type</div>
                <span className='break-words text-base'>{currentJobTemplate?.jobType}</span>
              </div>
              <div className='my-4'>
                <div className='text-sm text-slate-400'>Executable</div>
                <span className='break-words text-base'>{currentJobTemplate?.executable}</span>
              </div>
              <div className='my-4'>
                <div className='text-sm text-slate-400'>Comment</div>
                <span className='break-words text-base'>{currentJobTemplate?.comment || '-'}</span>
              </div>
              <div className='my-4'>
                <div className='text-sm text-slate-400'>Arguments</div>
                <span className='break-words text-base'>
                  {currentJobTemplate?.arguments.length === 1
                    ? currentJobTemplate?.arguments[0]
                    : currentJobTemplate?.arguments.length
                      ? currentJobTemplate?.arguments.join(', ')
                      : '-'}
                </span>
              </div>
              <div className='my-4'>
                <div className='mb-1 text-sm text-slate-400'>Environment Variable(s)</div>
                <Space.Compact className='max-h-80 w-full overflow-auto'>
                  <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                    <span className='bg-gray-100 p-1'>Env Var Name</span>
                    {currentJobTemplate?.environments
                      ? Object.keys(currentJobTemplate?.environments).map(envName => (
                          <span key={envName} className='truncate p-1' title={envName}>
                            {envName}
                          </span>
                        ))
                      : null}
                  </Space.Compact>
                  <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                    <span className='bg-gray-100 p-1'>Env Var Value</span>
                    {currentJobTemplate?.environments
                      ? Object.values(currentJobTemplate?.environments).map(envValue => (
                          <span key={envValue} className='truncate p-1' title={envValue}>
                            {envValue || '-'}
                          </span>
                        ))
                      : null}
                  </Space.Compact>
                </Space.Compact>
              </div>
              <div className='my-4'>
                <div className='mb-1 text-sm text-slate-400'>Custom Fields</div>
                <Space.Compact className='max-h-80 w-full overflow-auto'>
                  <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                    <span className='bg-gray-100 p-1'>Key</span>
                    {currentJobTemplate?.customFields
                      ? Object.keys(currentJobTemplate?.customFields).map(field => (
                          <span key={field} className='truncate p-1' title={field}>
                            {field}
                          </span>
                        ))
                      : null}
                  </Space.Compact>
                  <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                    <span className='bg-gray-100 p-1'>Value</span>
                    {currentJobTemplate?.customFields
                      ? Object.values(currentJobTemplate?.customFields).map(value => (
                          <span key={value} className='truncate p-1' title={value}>
                            {value || '-'}
                          </span>
                        ))
                      : null}
                  </Space.Compact>
                </Space.Compact>
              </div>
              {currentJobTemplate?.jobType === 'shell' && (
                <div className='my-4'>
                  <div className='text-sm text-slate-400'>Script(s)</div>
                  {currentJobTemplate?.scripts && currentJobTemplate.scripts.length > 0 ? (
                    currentJobTemplate.scripts.map((script, index) => (
                      <span key={index} className='mb-2 block break-words last:mb-0'>
                        {script}
                      </span>
                    ))
                  ) : (
                    <span>-</span>
                  )}
                </div>
              )}
              {currentJobTemplate?.jobType === 'spark' && (
                <>
                  <div className='my-4'>
                    <div className='text-sm text-slate-400'>Class Name</div>
                    <span className='break-words text-base'>{currentJobTemplate?.className}</span>
                  </div>
                  <div className='my-4'>
                    <div className='text-sm text-slate-400'>Jars</div>
                    {currentJobTemplate?.jars && currentJobTemplate.jars.length > 0 ? (
                      currentJobTemplate.jars.map((jar, index) => (
                        <span key={index} className='mb-2 block break-words last:mb-0'>
                          {jar}
                        </span>
                      ))
                    ) : (
                      <span>-</span>
                    )}
                  </div>
                  <div className='my-4'>
                    <div className='text-sm text-slate-400'>Files</div>
                    {currentJobTemplate?.files && currentJobTemplate.files.length > 0 ? (
                      currentJobTemplate.files.map((file, index) => (
                        <span key={index} className='mb-2 block break-words last:mb-0'>
                          {file}
                        </span>
                      ))
                    ) : (
                      <span>-</span>
                    )}
                  </div>
                  <div className='my-4'>
                    <div className='text-sm text-slate-400'>Archives</div>
                    {currentJobTemplate?.archives && currentJobTemplate.archives.length > 0 ? (
                      currentJobTemplate.archives.map((archive, index) => (
                        <span key={index} className='mb-2 block break-words last:mb-0'>
                          {archive}
                        </span>
                      ))
                    ) : (
                      <span>-</span>
                    )}
                  </div>
                  <div className='my-4'>
                    <div className='mb-1 text-sm text-slate-400'>Config(s)</div>
                    <Space.Compact className='max-h-80 w-full overflow-auto'>
                      <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                        <span className='bg-gray-100 p-1'>Config Name</span>
                        {currentJobTemplate?.configs
                          ? Object.keys(currentJobTemplate?.configs).map(configName => (
                              <span key={configName} className='truncate p-1' title={configName}>
                                {configName}
                              </span>
                            ))
                          : null}
                      </Space.Compact>
                      <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                        <span className='bg-gray-100 p-1'>Config Value</span>
                        {currentJobTemplate?.configs
                          ? Object.values(currentJobTemplate?.configs).map(configValue => (
                              <span key={configValue} className='truncate p-1' title={configValue}>
                                {configValue || '-'}
                              </span>
                            ))
                          : null}
                      </Space.Compact>
                    </Space.Compact>
                  </div>
                </>
              )}
            </>
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
