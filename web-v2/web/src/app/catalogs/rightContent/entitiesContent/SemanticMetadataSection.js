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

import { Button, Collapse, theme } from 'antd'

export default function SemanticMetadataSection({ label, onEdit, children }) {
  const { token } = theme.useToken()

  return (
    <Collapse
      size='small'
      className='mb-3'
      items={[
        {
          key: 'metadata',
          label: (
            <span
              className='inline-flex min-h-6 items-center'
              style={{ fontSize: token.fontSize, fontWeight: token.fontWeightStrong, color: token.colorText }}
            >
              {label}
            </span>
          ),
          extra: onEdit && (
            <Button
              size='small'
              type='link'
              aria-label={`Edit ${label}`}
              onClick={event => {
                event.stopPropagation()
                onEdit()
              }}
            >
              Edit
            </Button>
          ),
          children
        }
      ]}
    />
  )
}
