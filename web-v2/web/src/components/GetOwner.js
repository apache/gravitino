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

import React, { useEffect } from 'react'
import { getCurrentEntityOwner } from '@/lib/store/metalakes'
import { useAppDispatch } from '@/lib/hooks/useStore'

export default function GetOwner({ metalake, metadataObjectType, metadataObjectFullName, refreshKey }) {
  const [ownerData, setOwnerData] = React.useState(null)
  const dispatch = useAppDispatch()

  useEffect(() => {
    const fetchOwnerData = async () => {
      const { payload } = await dispatch(
        getCurrentEntityOwner({ metalake, metadataObjectType, metadataObjectFullName })
      )
      setOwnerData(payload?.owner)
    }
    if (metalake && metadataObjectType && metadataObjectFullName) {
      fetchOwnerData()
    }
  }, [refreshKey, metalake, metadataObjectType, metadataObjectFullName])

  return <span>{ownerData?.name || '-'}</span>
}
