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

import { useEffect, useRef, useState } from 'react'
import { Box } from '@mui/material'
import dynamic from 'next/dynamic'
import Loading from '@/app/rootLayout/Loading'

const DynamicMetalakeTree = dynamic(() => import('./MetalakeTree'), {
  loading: () => <Loading />,
  ssr: false
})

const MetalakePageLeftBar = () => {
  const treeWrapper = useRef(null)

  const [height, setHeight] = useState(0)

  useEffect(() => {
    if (treeWrapper.current) {
      const { offsetHeight } = treeWrapper.current
      setHeight(offsetHeight - 20)
    }
  }, [treeWrapper])

  return (
    <Box
      className={`twc-overflow-y-hidden`}
      sx={{ p: theme => theme.spacing(2.5, 2.5, 2.5), height: `calc(100% - 1.5rem)` }}
      ref={treeWrapper}
    >
      <DynamicMetalakeTree height={height} />
    </Box>
  )
}

export default MetalakePageLeftBar
