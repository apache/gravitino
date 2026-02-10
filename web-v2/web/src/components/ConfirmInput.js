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

import { useState, useEffect, forwardRef, useImperativeHandle } from 'react'
import { Input, Typography } from 'antd'

const { Paragraph } = Typography

const ConfirmInput = forwardRef(function ConfirmInput(props, ref) {
  const { name, setConfirmInput, isManaged, location, type, confirmTips, notMatchTips, registerValidate, isInUse } =
    props
  const [value, setValue] = useState('')
  const [showError, setShowError] = useState(false)
  const [inUseError, setInUseError] = useState(false)
  const doubleCheck = confirmTips || `Please enter "${name}" to confirm the deletion.`

  useImperativeHandle(ref, () => ({
    validate: () => {
      if (isInUse) {
        setInUseError(true)

        return false
      }
      if (value !== name) {
        setShowError(true)

        return false
      }

      return true
    }
  }))

  // Register validate function via callback to solve the issue that ref cannot be passed with dynamic import
  useEffect(() => {
    if (registerValidate) {
      registerValidate(() => {
        if (isInUse) {
          setInUseError(true)

          return false
        }
        if (value !== name) {
          setShowError(true)

          return false
        }

        return true
      })
    }
  }, [registerValidate, value, name, isInUse])

  const getStatus = () => {
    if (showError && value === '') return 'error'
    if (value !== '' && value !== name) return 'error'

    return ''
  }

  return (
    <>
      {isManaged && (
        <Paragraph
          style={{ marginBottom: '0.5rem' }}
        >{`Please note, the storage location "${location}" will be deleted as well.`}</Paragraph>
      )}
      {type === 'metalake' && (
        <Paragraph
          style={{ marginBottom: '0.5rem' }}
        >{`Make sure the ${type} ${name} is not in-use, and all sub-entities in it are dropped.`}</Paragraph>
      )}
      <Paragraph style={{ marginBottom: '0.5rem' }}>{doubleCheck}</Paragraph>
      <Input
        data-refer='confirm-delete-input'
        value={value}
        status={getStatus()}
        onChange={e => {
          setValue(e.target.value)
          setConfirmInput(e.target.value)
          if (showError) setShowError(false)
        }}
      />
      {showError && value === '' && (
        <Paragraph type='danger' style={{ marginBottom: 0, marginTop: '0.25rem', fontSize: '12px' }}>
          {confirmTips || `Please enter the name to confirm deletion`}
        </Paragraph>
      )}
      {value !== '' && value !== name && (
        <Paragraph type='danger' style={{ marginBottom: 0, marginTop: '0.25rem', fontSize: '12px' }}>
          {notMatchTips || `The entered name does not match`}
        </Paragraph>
      )}
      {inUseError && (
        <Paragraph type='danger' style={{ marginBottom: 0, marginTop: '0.25rem', fontSize: '12px' }}>
          Please set the {type} to &quot;Not In-Use&quot; before deleting
        </Paragraph>
      )}
    </>
  )
})

export default ConfirmInput
