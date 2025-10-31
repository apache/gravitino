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

import React, { useEffect, useState } from 'react'
import { Form, Modal, Spin, Typography } from 'antd'
import CascaderObjectComponent from '@/components/CascaderObjectComponent'
import { validateMessages } from '@/config'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'

const { Paragraph } = Typography

const defaultValues = {
  securableObjects: [{ fullName: '', type: '', allowPrivileges: [], denyPrivileges: [] }]
}

export default function GrantPrivilegesForRoleDialog({ ...props }) {
  const { open, setOpen, role, metalake } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [cacheData, setCacheData] = useState({})

  const [form] = Form.useForm()
  const values = Form.useWatch([], form)

  useResetFormOnCloseModal({
    form,
    open
  })

  // const { trigger: getEditRole } = useRoleAsync()

  useEffect(() => {
    if (open && role) {
      const init = async () => {
        setIsLoading(true)
        try {
          const { role: roleObj } = await getEditRole({ params: { metalake, role } })
          const securableObjectsMap = {}
          roleObj.securableObjects.forEach((object, index) => {
            securableObjectsMap[object.fullName] = [
              object.privileges.filter(p => p.condition === 'allow').map(p => p.name),
              object.privileges.filter(p => p.condition !== 'allow').map(p => p.name)
            ]
          })
          setCacheData(securableObjectsMap)
          setIsLoading(false)
        } catch (e) {
          console.error(e)
          setIsLoading(false)
        }
      }
      init()
    }
  }, [open, role, form])

  // const { trigger: revokePrivilege, isMutating: isRevokeMutating } = useRevokePrivilegeAsync()
  // const { trigger: grantPrivilege, isMutating: isGrantMutating } = useGrantPrivilegeAsync()

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)
        values.securableObjects.forEach(async object => {
          const type = object.type
          let fullName = object.fullName.at(-1)
          if (type === 'schema') {
            fullName = `${object.fullName.at(-2)}.${object.fullName.at(-1)}`
          }
          if (['table', 'fileset', 'topic'].includes(type)) {
            fullName = `${object.fullName.at(-3)}.${object.fullName.at(-2)}.${object.fullName.at(-1)}`
          }
          const params = { metalake, role, metadataObjectType: type, metadataObjectFullName: fullName }
          const [allowPrivileges, denyPrivileges] = cacheData?.[fullName] || []
          let revokePrivileges = []
          if (allowPrivileges?.length || denyPrivileges?.length) {
            const revokeAllow = allowPrivileges
              .filter(p => !object.allowPrivileges.includes(p))
              .map(p => ({ name: p, condition: 'allow' }))

            const revokeDeny = denyPrivileges
              .filter(p => !object.denyPrivileges.includes(p))
              .map(p => ({ name: p, condition: 'deny' }))
            revokePrivileges = [...revokeAllow, ...revokeDeny]
          }

          const grantAllow = object.allowPrivileges
            .filter(p => !allowPrivileges?.includes(p))
            .map(p => ({ name: p, condition: 'allow' }))

          const grantDeny = object.denyPrivileges
            .filter(p => !denyPrivileges?.includes(p))
            .map(p => ({ name: p, condition: 'deny' }))
          const grantPrivileges = [...grantAllow, ...grantDeny]
          if (revokePrivileges.length) {
            await revokePrivilege({
              params,
              data: { privileges: revokePrivileges }
            })
          }
          if (grantPrivileges.length) {
            await grantPrivilege({
              params,
              data: { privileges: grantPrivileges }
            })
          }
        })
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

  const renderPrivileges = (fields, subOpt) => {
    return (
      <div className='flex flex-col divide-y divide-solid border-b border-solid'>
        <div className='grid grid-cols-7 divide-x divide-solid'>
          <div className='col-span-3 bg-gray-100 p-1 text-center'>Object</div>
          <div className='col-span-4 bg-gray-100 p-1 text-center'>Privileges</div>
        </div>
        {fields.map(subField => (
          <div key={subField.key}>
            <div className='grid grid-cols-7'>
              <CascaderObjectComponent form={form} subField={subField} metalake={metalake} cacheData={cacheData} />
            </div>
          </div>
        ))}
      </div>
    )
  }

  return (
    <>
      <Modal
        title={`Grant privileges for role ${role}`}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={1000}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>{!role ? 'Create a new role' : `Update Role ${role} Privileges`}</Paragraph>
        <Spin spinning={isLoading}>
          <Form
            form={form}
            initialValues={defaultValues}
            layout='vertical'
            name='grantPrivilegesForm'
            validateMessages={validateMessages}
          >
            <Form.Item label='' name='securableObjects'>
              <Form.List name='securableObjects'>{(fields, subOpt) => renderPrivileges(fields, subOpt)}</Form.List>
            </Form.Item>
          </Form>
        </Spin>
      </Modal>
    </>
  )
}
