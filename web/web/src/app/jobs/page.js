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
import { createContext, useContext, useMemo, useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { useRouter, useSearchParams } from 'next/navigation'
import { ArrowRightOutlined, ExclamationCircleFilled, PlusOutlined, RedoOutlined } from '@ant-design/icons'
import { Button, Drawer, Flex, Input, Modal, Spin, Table, Tag, Tooltip, Typography, theme } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import ConfirmInput from '@/components/ConfirmInput'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import { jobStatusEnum } from '@/config'
import Link from 'next/link'
import { formatToDateTime } from '@/lib/utils'
import Loading from '@/components/Loading'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { fetchJobs, fetchJobTemplates, getJobDetails, cancelJob } from '@/lib/store/jobs'

const RegisterJobTemplateDialog = dynamic(() => import('./RegisterJobTemplateDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const CreateJobDialog = dynamic(() => import('./CreateJobDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

export default function JobsPage() {
  const [open, setOpen] = useState(false)
  const [openJobTemplate, setOpenJobTemplate] = useState(false)
  const router = useRouter()
  const [modal, contextHolder] = Modal.useModal()
  const [search, setSearch] = useState('')
  const { token } = theme.useToken()
  const [currentJob, setCurrentJob] = useState(null)
  const [openDetailJob, setOpenDetailJob] = useState(false)
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.jobs)
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const [jobDetailLoading, setJobDetailLoading] = useState(false)

  useEffect(() => {
    if (currentMetalake) {
      dispatch(fetchJobs({ metalake: currentMetalake }))
      dispatch(fetchJobTemplates({ metalake: currentMetalake }))
    }
  }, [dispatch, currentMetalake])

  const tableData = store.jobsData
    ?.filter(j => {
      if (search === '') return true

      return j.jobId.includes(search)
    })
    .map(job => {
      return {
        ...job,
        key: job.jobId
      }
    })

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleCreateJob = templates => {
    if (!templates || templates.length === 0) {
      setOpenJobTemplate(true)
    } else {
      setOpen(true)
    }
  }

  const handleViewJobDetail = async jobId => {
    setJobDetailLoading(true)
    const { payload: jobDetail } = await dispatch(getJobDetails({ metalake: currentMetalake, jobId }))
    setCurrentJob(jobDetail)
    setOpenDetailJob(true)
    setJobDetailLoading(false)
  }

  const onClose = () => {
    setOpenDetailJob(false)
    setCurrentJob(null)
  }

  const showDeleteConfirm = (NameContext, jobId) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }
    modal.confirm({
      title: `Are you sure to cancel the job ${jobId}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContext.Consumer>
          {jobId => (
            <ConfirmInput
              name={jobId}
              setConfirmInput={setConfirmInput}
              confirmTips={`Please enter \"${jobId}\" to confirm cancelling the job!`}
              notMatchTips={`The entered id does not match the job ID`}
              registerValidate={registerValidate}
            />
          )}
        </NameContext.Consumer>
      ),
      okText: 'Cancel Job',
      okType: 'danger',
      cancelText: 'Close',
      onOk(close) {
        if (validateFn && !validateFn()) return

        const confirmFn = async () => {
          await dispatch(cancelJob({ metalake: currentMetalake, jobId }))
          close()
        }
        confirmFn()
      }
    })
  }

  const getStatusColor = status => {
    let color = ''
    switch (status) {
      case jobStatusEnum.queued:
        color = 'default'
        break
      case jobStatusEnum.started:
        color = token.colorPrimary
        break
      case jobStatusEnum.failed:
        color = 'red'
        break
      case jobStatusEnum.succeeded:
        color = 'green'
        break
      case jobStatusEnum.cancelling:
        color = 'orange'
        break
      case jobStatusEnum.cancelled:
        color = 'gray'
        break
      default:
        color = 'default'
    }

    return color
  }

  const columns = useMemo(
    () => [
      {
        title: 'Job ID',
        dataIndex: 'jobId',
        key: 'jobId',
        ellipsis: true,
        width: 200
      },
      {
        title: 'Status',
        dataIndex: 'status',
        key: 'status',
        ellipsis: true,
        width: 120,
        filters: jobStatusEnum ? Object.values(jobStatusEnum).map(status => ({ text: status, value: status })) : [],
        onFilter: (value, record) => record.status.indexOf(value) === 0,
        render: (_, record, index) => {
          const status = record.status
          const color = getStatusColor(status)

          return (
            <Tag color={color} key={index}>
              {status}
            </Tag>
          )
        }
      },
      {
        title: (
          <span className='flex items-center justify-between'>
            Template Name
            <Tooltip title='Go to Job Template'>
              <ArrowRightOutlined
                className='cursor-pointer'
                onClick={() => {
                  router.push(`/jobTemplates?metalake=${currentMetalake}`)
                }}
              />
            </Tooltip>
          </span>
        ),
        dataIndex: 'jobTemplateName',
        key: 'jobTemplateName',
        ellipsis: true,
        width: 180,
        render: (_, record) => (
          <Link
            href={`/jobTemplates?jobTemplateName=${record.jobTemplateName}&metalake=${currentMetalake}`}
            title={record.jobTemplateName}
          >
            {record.jobTemplateName}
          </Link>
        )
      },
      {
        title: 'Creator',
        dataIndex: ['audit', 'creator'],
        key: 'creator',
        ellipsis: true
      },
      {
        title: 'Create At',
        dataIndex: ['audit', 'createTime'],
        key: 'createTime',
        ellipsis: true,
        render: (_, record) => <>{formatToDateTime(record.audit.createTime)}</>
      },
      {
        title: 'Actions',
        key: 'actions',
        width: 100,
        render: (_, record) => {
          const NameContext = createContext(record.jobId)

          return (
            <div className='flex gap-2'>
              <NameContext.Provider value={record.jobId}>{contextHolder}</NameContext.Provider>
              <a>
                <Tooltip title='View Details'>
                  <Icons.Eye className='size-4' onClick={() => handleViewJobDetail(record.jobId)} />
                </Tooltip>
              </a>
              {![
                jobStatusEnum.succeeded,
                jobStatusEnum.failed,
                jobStatusEnum.cancelled,
                jobStatusEnum.cancelling
              ].includes(record.status) && (
                <a>
                  <Tooltip title='Cancel Job'>
                    <Icons.CircleX className='size-4' onClick={() => showDeleteConfirm(NameContext, record.jobId)} />
                  </Tooltip>
                </a>
              )}
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
        <Title level={2}>Jobs</Title>
        <Paragraph type='secondary'>This table lists the jobs you have access to.</Paragraph>
        <Flex justify='flex-end' className='mb-4'>
          <div className='flex w-1/2 gap-4'>
            <Search name='searchCatalogInput' placeholder='Search...' onChange={onSearchTable} />
            <Button
              icon={<RedoOutlined />}
              onClick={() => dispatch(fetchJobs({ metalake: currentMetalake }))}
              className='w-fit'
            >
              Refresh
            </Button>
            <Button
              type='primary'
              icon={<PlusOutlined />}
              onClick={() => handleCreateJob(store.jobTemplateNames || [])}
              className='w-fit'
            >
              {store.jobTemplateNames?.length > 0 ? 'Run Job' : 'Register Job Template'}
            </Button>
          </div>
        </Flex>
        <Spin spinning={store.isJobsLoading}>
          <Table
            style={{ maxHeight: 'calc(100vh - 25rem)' }}
            scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
            dataSource={tableData}
            columns={resizableColumns}
            components={components}
            pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
          />
        </Spin>
        {openDetailJob && (
          <Drawer
            title={`View Job ${currentJob?.jobId} Details`}
            loading={jobDetailLoading}
            onClose={onClose}
            open={openDetailJob}
          >
            <>
              <div className='my-4'>
                <div className='text-sm text-slate-400'>Job ID</div>
                <span className='break-words text-base'>{currentJob?.jobId}</span>
              </div>
              <div className='my-4'>
                <div className='text-sm text-slate-400'>Template Name</div>
                <span className='break-words text-base'>{currentJob?.jobTemplateName}</span>
              </div>
              <div className='my-4'>
                <div className='text-sm text-slate-400'>Status</div>
                <span className='break-words text-base'>
                  {<Tag color={getStatusColor(currentJob?.status || '')}>{currentJob?.status}</Tag>}
                </span>
              </div>
              <div className='my-4'>
                <div className='text-sm text-slate-400'>Details</div>
                <div className='flex justify-between'>
                  <span className='text-sm'>Creator: </span>
                  <span className='text-sm'>{currentJob?.audit?.creator}</span>
                </div>
                <div className='flex justify-between'>
                  <span className='text-sm'>Created At: </span>
                  <span className='text-sm'>{formatToDateTime(currentJob?.audit?.createTime)}</span>
                </div>
                <div className='flex justify-between'>
                  <span className='text-sm'>Updated At: </span>
                  <span className='text-sm'>{formatToDateTime(currentJob?.audit?.lastModifiedTime)}</span>
                </div>
              </div>
            </>
          </Drawer>
        )}
        {open && (
          <CreateJobDialog
            open={open}
            setOpen={setOpen}
            metalake={currentMetalake}
            router={router}
            jobTemplateNames={store.jobTemplateNames}
          />
        )}
        {openJobTemplate && (
          <RegisterJobTemplateDialog open={openJobTemplate} setOpen={setOpenJobTemplate} metalake={currentMetalake} />
        )}
      </div>
    </SectionContainer>
  )
}
