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

import Link from 'next/link'

import { Box, Typography } from '@mui/material'

const Footer = props => {
  return (
    <Box component={'footer'} className={'layout-footer  twc-z-10 twc-flex twc-items-center twc-justify-center'}>
      <Box className='footer-content-wrapper twc-px-6 twc-w-full twc-py-[0.75rem] [@media(min-width:1440px)]:twc-max-w-[1440px]'>
        <Box className={'twc-flex twc-flex-wrap twc-items-center twc-justify-between'}>
          <Typography className='twc-mr-2'>
            {`© 2024 `}
            <Link
              className={'twc-no-underline twc-text-primary-main'}
              target='_blank'
              href='https://gravitino.apache.org/'
              data-refer='footer-link-gravitino'
            >
              ASF
            </Link>
          </Typography>
          <Box className={'twc-flex twc-flex-wrap twc-items-center [&>:not(:last-child)]:twc-mr-4'}>
            <Link
              className={'twc-no-underline twc-text-primary-main'}
              target='_blank'
              href='https://github.com/apache/gravitino/blob/main/LICENSE'
              data-refer='footer-link-license'
            >
              License
            </Link>
            <Link
              className={'twc-no-underline twc-text-primary-main'}
              target='_blank'
              href='https://gravitino.apache.org/docs/latest/'
              data-refer='footer-link-docs'
            >
              Documentation
            </Link>
            <Link
              className={'twc-no-underline twc-text-primary-main'}
              target='_blank'
              href='https://github.com/apache/gravitino/issues'
              data-refer='footer-link-support'
            >
              Support
            </Link>
          </Box>
        </Box>
      </Box>
    </Box>
  )
}

export default Footer
