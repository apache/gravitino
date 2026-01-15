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

import React, { useEffect, useRef, useState } from 'react'
import { PlusOutlined } from '@ant-design/icons'
import { Button, Flex, Form, Input, Modal, Radio, Select, Spin, Typography } from 'antd'
import { useScrolling } from 'react-use'
import Icons from '@/components/Icons'
import { dialogContentMaxHeigth, validateMessages, mismatchName } from '@/config'
import { nameRegex } from '@/lib/utils/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import { createJobTemplate, getJobTemplate, updateJobTemplate } from '@/lib/store/jobs'
import { useAppDispatch } from '@/lib/hooks/useStore'

const { Paragraph } = Typography
const { TextArea } = Input

const defaultValues = {
  name: '',
  jobType: 'shell',
  comment: '',
  executable: '',
  arguments: [],
  environments: [],
  customFields: [],
  scripts: []
}

export default function RegisterJobTemplateDialog({ ...props }) {
  const { open, setOpen, metalake, editJobTemplate, details } = props
  const [isLoading, setIsLoading] = useState(false)
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
  const scrollRef = useRef(null)
  const loadedRef = useRef(false)
  const scrolling = useScrolling(scrollRef)
  const [bottomShadow, setBottomShadow] = useState(false)
  const [topShadow, setTopShadow] = useState(false)
  const [form] = Form.useForm()
  const values = Form.useWatch([], form)
  const jobType = Form.useWatch('jobType', form)
  const dispatch = useAppDispatch()

  const handScroll = () => {
    if (scrollRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = scrollRef.current
      if (scrollHeight > clientHeight + scrollTop) {
        setTopShadow(true)
        setBottomShadow(scrollTop > 0)
      } else if (scrollHeight === clientHeight + scrollTop) {
        setTopShadow(false)
        setBottomShadow(scrollTop > 0)
      }
    }
  }

  useEffect(() => {
    scrollRef.current && handScroll()
  }, [scrolling])

  useResetFormOnCloseModal({
    form,
    open
  })

  useEffect(() => {
    const initLoad = async () => {
      setIsLoading(true)
      try {
        const { payload: jobTemplateDetail } = await dispatch(
          getJobTemplate({ metalake, jobTemplate: editJobTemplate })
        )
        setCacheData(jobTemplateDetail)
        form.setFieldValue('name', jobTemplateDetail.name)
        form.setFieldValue('jobType', jobTemplateDetail.jobType)
        form.setFieldValue('comment', jobTemplateDetail.comment)
        form.setFieldValue('executable', jobTemplateDetail.executable)
        form.setFieldValue('arguments', jobTemplateDetail.arguments)
        let index = 0
        Object.entries(jobTemplateDetail.environments || {}).forEach(([key, value]) => {
          form.setFieldValue(['environments', index, 'envName'], key)
          form.setFieldValue(['environments', index, 'envValue'], value)
          index++
        })
        let customFieldIndex = 0
        Object.entries(jobTemplateDetail.customFields || {}).forEach(([key, value]) => {
          form.setFieldValue(['customFields', customFieldIndex, 'fieldName'], key)
          form.setFieldValue(['customFields', customFieldIndex, 'fieldValue'], value)
          customFieldIndex++
        })
        if (jobTemplateDetail.jobType === 'shell') {
          form.setFieldValue('scripts', jobTemplateDetail.scripts)
        } else {
          form.setFieldValue('className', jobTemplateDetail.className)
          form.setFieldValue('jars', jobTemplateDetail.jars)
          form.setFieldValue('files', jobTemplateDetail.files)
          form.setFieldValue('archives', jobTemplateDetail.archives)
          let configIndex = 0
          Object.entries(jobTemplateDetail.configs || {}).forEach(([key, value]) => {
            form.setFieldValue(['configs', configIndex, 'configName'], key)
            form.setFieldValue(['configs', configIndex, 'configValue'], value)
            configIndex++
          })
        }
        setIsLoading(false)
      } catch (e) {
        setIsLoading(false)
      }
    }
    if (open && editJobTemplate && !loadedRef.current) {
      loadedRef.current = true
      initLoad()
    }

    // Reset loadedRef when dialog closes
    if (!open) {
      loadedRef.current = false
    }
  }, [open, editJobTemplate, metalake])

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)

        const submitData = {
          name: values.name.trim(),
          comment: values.comment,
          jobType: values.jobType,
          executable: values.executable,
          arguments: values.arguments,
          environments: values.environments?.reduce((acc, item) => {
            if (item && item.envName && item.envValue) {
              acc[item.envName] = item.envValue
            }

            return acc
          }, {}),
          customFields: values.customFields?.reduce((acc, item) => {
            if (item && item.fieldName && item.fieldValue) {
              acc[item.fieldName] = item.fieldValue
            }

            return acc
          }, {}),
          scripts: values.scripts
        }
        if (jobType === 'spark') {
          Object.assign(submitData, {
            className: values.className,
            jars: values.jars,
            files: values.files,
            archives: values.archives,
            configs: values.configs?.reduce((acc, item) => {
              if (item && item.configName && item.configValue) {
                acc[item.configName] = item.configValue
              }

              return acc
            }, {})
          })
        }
        if (editJobTemplate) {
          // update job template
          const reqData = { updates: genUpdates(cacheData, submitData) }
          if (reqData.updates.length) {
            await dispatch(updateJobTemplate({ metalake, jobTemplate: editJobTemplate, data: reqData }))
          }
        } else {
          await dispatch(createJobTemplate({ metalake, data: { jobTemplate: submitData }, details }))
        }
        setConfirmLoading(false)
        setOpen(false)
      })
      .catch(info => {
        console.error(info)
        form.scrollToField(info?.errorFields?.[0]?.name?.[0])
      })
  }

  const handleCancel = () => {
    setOpen(false)
  }

  return (
    <>
      <Modal
        title={!editJobTemplate ? 'Register Job Template' : 'Edit Job Template'}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>
          {!editJobTemplate ? 'Register a new job template.' : `Edit the job template ${editJobTemplate}`}
        </Paragraph>
        <div
          className={cn('relative', {
            'after:absolute after:-bottom-10 after:left-0 after:right-0 after:h-10 after:shadow-[0px_-10px_8px_-8px_rgba(5,5,5,0.1)]':
              topShadow,
            'before:absolute before:-top-10 before:left-0 before:right-0 before:h-10 before:z-10 before:shadow-[0px_10px_8px_-8px_rgba(5,5,5,0.1)]':
              bottomShadow
          })}
        >
          <div className='overflow-auto' style={{ maxHeight: `${dialogContentMaxHeigth}px` }} ref={scrollRef}>
            <Spin spinning={isLoading}>
              <Form
                form={form}
                initialValues={defaultValues}
                layout='vertical'
                name='policyForm'
                validateMessages={validateMessages}
              >
                <Form.Item
                  name='name'
                  label='Template Name'
                  rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
                  messageVariables={{ label: 'template name' }}
                >
                  <Input placeholder={mismatchName} />
                </Form.Item>
                <Form.Item name='jobType' label='Job Type'>
                  <Radio.Group disabled={editJobTemplate}>
                    <Radio value={'shell'}>{'Shell'}</Radio>
                    <Radio value={'spark'}>{'Spark'}</Radio>
                  </Radio.Group>
                </Form.Item>
                <Form.Item name='comment' label='Comment'>
                  <TextArea />
                </Form.Item>
                <Form.Item
                  name='executable'
                  rules={[{ required: true, message: 'Please enter the executable!' }]}
                  label='Executable'
                >
                  <Input placeholder='e.g. /path/to/my_script.sh' />
                </Form.Item>
                <Form.Item name='arguments' label='Arguments'>
                  <Select mode='tags' tokenSeparators={[',']} placeholder='e.g. {{arg1}},{{arg2}}' />
                </Form.Item>
                <Form.Item label='Environment Variables'>
                  <div className='flex flex-col gap-2'>
                    <Form.List name='environments'>
                      {(fields, { add, remove }) => (
                        <>
                          {fields.map(({ key, name, ...restField }) => (
                            <Form.Item label={null} className='align-items-center mb-1' key={key}>
                              <Flex gap='small' align='start'>
                                <Form.Item
                                  {...restField}
                                  name={[name, 'envName']}
                                  rules={[{ required: true, message: 'Please enter environment variable name!' }]}
                                  className='mb-0 w-full grow'
                                >
                                  <Input placeholder='Env Var Name' />
                                </Form.Item>
                                <Form.Item
                                  {...restField}
                                  name={[name, 'envValue']}
                                  rules={[{ required: true, message: 'Please enter environment variable value!' }]}
                                  className='mb-0 w-full grow'
                                >
                                  <Input placeholder='Env Var Value' />
                                </Form.Item>
                                <Form.Item className='mb-0 grow-0'>
                                  <Icons.Minus
                                    className='size-8 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                    onClick={() => remove(name)}
                                  />
                                </Form.Item>
                              </Flex>
                            </Form.Item>
                          ))}
                          <Button className='w-fit' type='dashed' onClick={() => add()} icon={<PlusOutlined />}>
                            Add Env Var
                          </Button>
                        </>
                      )}
                    </Form.List>
                  </div>
                </Form.Item>
                <Form.Item label='Custom Field(s)'>
                  <div className='flex flex-col gap-2'>
                    <Form.List name='customFields'>
                      {(fields, { add, remove }) => (
                        <>
                          {fields.map(({ key, name, ...restField }) => (
                            <Form.Item label={null} className='align-items-center mb-1' key={key}>
                              <Flex gap='small' align='start'>
                                <Form.Item
                                  {...restField}
                                  name={[name, 'fieldName']}
                                  rules={[{ required: true, message: 'Please enter custom field name!' }]}
                                  className='mb-0 w-full grow'
                                >
                                  <Input placeholder='Field Name' />
                                </Form.Item>
                                <Form.Item
                                  {...restField}
                                  name={[name, 'fieldValue']}
                                  rules={[{ required: true, message: 'Please enter custom field value!' }]}
                                  className='mb-0 w-full grow'
                                >
                                  <Input placeholder='Field Value' />
                                </Form.Item>
                                <Form.Item className='mb-0 grow-0'>
                                  <Icons.Minus
                                    className='size-8 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                    onClick={() => remove(name)}
                                  />
                                </Form.Item>
                              </Flex>
                            </Form.Item>
                          ))}
                          <Button className='w-fit' type='dashed' onClick={() => add()} icon={<PlusOutlined />}>
                            Add Custom Field
                          </Button>
                        </>
                      )}
                    </Form.List>
                  </div>
                </Form.Item>
                {jobType === 'shell' && (
                  <Form.Item name='scripts' label='Script(s)'>
                    <Select mode='tags' tokenSeparators={[',']} />
                  </Form.Item>
                )}
                {jobType === 'spark' && (
                  <>
                    <Form.Item
                      name='className'
                      rules={[{ required: true, message: 'Please enter the class name!' }]}
                      label='Class Name'
                    >
                      <Input placeholder='e.g. com.example.MySparkApp' />
                    </Form.Item>
                    <Form.Item name='jars' label='Jars'>
                      <Select
                        mode='tags'
                        tokenSeparators={[',']}
                        placeholder='e.g. /path/to/dependency1.jar,/path/to/dependency2.jar'
                      />
                    </Form.Item>
                    <Form.Item name='files' label='Files'>
                      <Select
                        mode='tags'
                        tokenSeparators={[',']}
                        placeholder='e.g. /path/to/file1.txt,/path/to/file2.txt'
                      />
                    </Form.Item>
                    <Form.Item name='archives' label='Archives'>
                      <Select
                        mode='tags'
                        tokenSeparators={[',']}
                        placeholder='e.g. /path/to/archive1.zip,/path/to/archive2.zip'
                      />
                    </Form.Item>
                    <Form.Item label='Configs'>
                      <div className='flex flex-col gap-2'>
                        <Form.List name='configs'>
                          {(fields, { add, remove }) => (
                            <>
                              {fields.map(({ key, name, ...restField }) => (
                                <Form.Item label={null} className='align-items-center mb-1' key={key}>
                                  <Flex gap='small' align='start'>
                                    <Form.Item
                                      {...restField}
                                      name={[name, 'configName']}
                                      rules={[{ required: true, message: 'Please enter config name!' }]}
                                      className='mb-0 w-full grow'
                                    >
                                      <Input placeholder='Config Name' />
                                    </Form.Item>
                                    <Form.Item
                                      {...restField}
                                      name={[name, 'configValue']}
                                      rules={[{ required: true, message: 'Please enter config value!' }]}
                                      className='mb-0 w-full grow'
                                    >
                                      <Input placeholder='Config Value' />
                                    </Form.Item>
                                    <Form.Item className='mb-0 grow-0'>
                                      <Icons.Minus
                                        className='size-8 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                        onClick={() => remove(name)}
                                      />
                                    </Form.Item>
                                  </Flex>
                                </Form.Item>
                              ))}
                              <Button className='w-fit' type='dashed' onClick={() => add()} icon={<PlusOutlined />}>
                                Add Config
                              </Button>
                            </>
                          )}
                        </Form.List>
                      </div>
                    </Form.Item>
                  </>
                )}
              </Form>
            </Spin>
          </div>
        </div>
      </Modal>
    </>
  )
}
