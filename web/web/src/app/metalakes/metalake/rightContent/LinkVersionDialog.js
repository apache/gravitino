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

import { useState, forwardRef, useEffect, Fragment } from 'react'

import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  Fade,
  FormControl,
  FormHelperText,
  Grid,
  IconButton,
  InputLabel,
  TextField,
  Typography
} from '@mui/material'

import Icon from '@/components/Icon'

import { useAppDispatch } from '@/lib/hooks/useStore'
import { linkVersion } from '@/lib/store/metalakes'

import * as yup from 'yup'
import { useForm, Controller, useFieldArray } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { groupBy } from 'lodash-es'
import { keyRegex } from '@/lib/utils/regex'
import { useSearchParams } from 'next/navigation'
import { useAppSelector } from '@/lib/hooks/useStore'

const defaultValues = {
  uri: '',
  aliases: [{ name: '' }],
  comment: '',
  propItems: []
}

const schema = yup.object().shape({
  uri: yup.string().required(),
  aliases: yup
    .array()
    .of(
      yup.object().shape({
        name: yup
          .string()
          .required('The alias is required')
          .test('not-number', 'Alias cannot be a number or a numeric string', value => {
            return value === undefined || isNaN(Number(value))
          })
      })
    )
    .test('unique', 'Alias must be unique', (aliases, ctx) => {
      const values = aliases?.filter(a => !!a.name).map(a => a.name)
      const duplicates = values.filter((value, index, self) => self.indexOf(value) !== index)

      if (duplicates.length > 0) {
        const duplicateIndex = values.lastIndexOf(duplicates[0])

        return ctx.createError({
          path: `aliases.${duplicateIndex}.name`,
          message: 'This alias is duplicated'
        })
      }

      return true
    }),
  propItems: yup.array().of(
    yup.object().shape({
      required: yup.boolean(),
      key: yup.string().required(),
      value: yup.string().when('required', {
        is: true,
        then: schema => schema.required()
      })
    })
  )
})

const Transition = forwardRef(function Transition(props, ref) {
  return <Fade ref={ref} {...props} />
})

const LinkVersionDialog = props => {
  const { open, setOpen, type = 'create', data = {} } = props
  const searchParams = useSearchParams()
  const metalake = searchParams.get('metalake')
  const catalog = searchParams.get('catalog')
  const schemaName = searchParams.get('schema')
  const catalogType = searchParams.get('type')
  const model = searchParams.get('model')
  const [innerProps, setInnerProps] = useState([])
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)
  const [cacheData, setCacheData] = useState()

  const {
    control,
    reset,
    watch,
    setValue,
    getValues,
    handleSubmit,
    trigger,
    formState: { errors }
  } = useForm({
    defaultValues,
    mode: 'all',
    resolver: yupResolver(schema)
  })

  const handleFormChange = ({ index, event }) => {
    let data = [...innerProps]
    data[index][event.target.name] = event.target.value

    if (event.target.name === 'key') {
      const invalidKey = !keyRegex.test(event.target.value)
      data[index].invalid = invalidKey
    }

    const nonEmptyKeys = data.filter(item => item.key.trim() !== '')
    const grouped = groupBy(nonEmptyKeys, 'key')
    const duplicateKeys = Object.keys(grouped).some(key => grouped[key].length > 1)

    if (duplicateKeys) {
      data[index].hasDuplicateKey = duplicateKeys
    } else {
      data.forEach(it => (it.hasDuplicateKey = false))
    }

    setInnerProps(data)
    setValue('propItems', data)
  }

  const addFields = () => {
    const duplicateKeys = innerProps.some(item => item.hasDuplicateKey)

    if (duplicateKeys) {
      return
    }

    let newField = { key: '', value: '', required: false }

    setInnerProps([...innerProps, newField])
    setValue('propItems', [...innerProps, newField])
  }

  const removeFields = index => {
    let data = [...innerProps]
    data.splice(index, 1)
    setInnerProps(data)
    setValue('propItems', data)
  }

  const { fields, append, remove } = useFieldArray({
    control,
    name: 'aliases'
  })

  const watchAliases = watch('aliases')

  const handleClose = () => {
    reset()
    setInnerProps([])
    setValue('propItems', [])
    setOpen(false)
  }

  const handleClickSubmit = e => {
    e.preventDefault()

    return handleSubmit(onSubmit(getValues()), onError)
  }

  const onSubmit = data => {
    const hasError = innerProps.some(prop => prop.hasDuplicateKey || prop.invalid)

    if (hasError) {
      return
    }

    trigger()

    schema
      .validate(data)
      .then(() => {
        const properties = innerProps.reduce((acc, item) => {
          acc[item.key] = item.value

          return acc
        }, {})

        const schemaData = {
          uri: data.uri,
          aliases: data.aliases.map(alias => alias.name),
          comment: data.comment,
          properties
        }

        if (type === 'create') {
          dispatch(
            linkVersion({ data: schemaData, metalake, catalog, schema: schemaName, type: catalogType, model })
          ).then(res => {
            if (!res.payload?.err) {
              handleClose()
            }
          })
        }
      })
      .catch(err => {
        console.error('valid error', err)
      })
  }

  const onError = errors => {
    console.error('fields error', errors)
  }

  useEffect(() => {
    if (open && JSON.stringify(data) !== '{}') {
      const { properties = {} } = data

      setCacheData(data)
      setValue('uri', data.uri)
      setValue('comment', data.comment)

      const propsItems = Object.entries(properties).map(([key, value]) => {
        return {
          key,
          value
        }
      })

      setInnerProps(propsItems)
      setValue('propItems', propsItems)
    }
  }, [open, data, setValue, type])

  return (
    <Dialog fullWidth maxWidth='sm' scroll='body' TransitionComponent={Transition} open={open} onClose={handleClose}>
      <form onSubmit={e => handleClickSubmit(e)}>
        <DialogContent
          sx={{
            position: 'relative',
            pb: theme => `${theme.spacing(8)} !important`,
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pt: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <IconButton
            size='small'
            onClick={() => handleClose()}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          >
            <Icon icon='bx:x' />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {type === 'create' ? 'Link' : 'Edit'} Version
            </Typography>
          </Box>

          <Grid container spacing={6}>
            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='uri'
                  control={control}
                  rules={{ required: true }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      label='URI'
                      onChange={onChange}
                      placeholder=''
                      disabled={type === 'update'}
                      error={Boolean(errors.uri)}
                      data-refer='link-uri-field'
                    />
                  )}
                />
                {errors.uri && <FormHelperText sx={{ color: 'error.main' }}>{errors.uri.message}</FormHelperText>}
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              {fields.map((field, index) => {
                return (
                  <Grid key={index} item xs={12} sx={{ '& + &': { mt: 2 } }}>
                    <FormControl fullWidth>
                      <Box
                        key={field.id}
                        sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}
                        data-refer={`version-aliases-${index}`}
                      >
                        <Box sx={{ flexGrow: 1 }}>
                          <Controller
                            name={`aliases.${index}.name`}
                            control={control}
                            render={({ field }) => (
                              <TextField
                                {...field}
                                onChange={event => {
                                  field.onChange(event)
                                  trigger('aliases')
                                }}
                                label={`Alias ${index + 1}`}
                                error={!!errors.aliases?.[index]?.name || !!errors.aliases?.message}
                                helperText={errors.aliases?.[index]?.name?.message || errors.aliases?.message}
                                fullWidth
                              />
                            )}
                          />
                        </Box>
                        <Box>
                          {index === 0 ? (
                            <Box sx={{ minWidth: 40 }}>
                              <IconButton onClick={() => append({ name: '' })}>
                                <Icon icon='mdi:plus-circle-outline' />
                              </IconButton>
                            </Box>
                          ) : (
                            <Box sx={{ minWidth: 40 }}>
                              <IconButton onClick={() => remove(index)}>
                                <Icon icon='mdi:minus-circle-outline' />
                              </IconButton>
                            </Box>
                          )}
                        </Box>
                      </Box>
                    </FormControl>
                  </Grid>
                )
              })}
            </Grid>

            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='comment'
                  control={control}
                  rules={{ required: false }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      label='Comment'
                      multiline
                      rows={2}
                      onChange={onChange}
                      placeholder=''
                      error={Boolean(errors.comment)}
                      data-refer='version-comment-field'
                    />
                  )}
                />
              </FormControl>
            </Grid>

            <Grid item xs={12} data-refer='version-props-layout'>
              <Typography sx={{ mb: 2 }} variant='body2'>
                Properties
              </Typography>
              {innerProps.map((item, index) => {
                return (
                  <Fragment key={index}>
                    <Grid item xs={12} sx={{ '& + &': { mt: 2 } }}>
                      <FormControl fullWidth>
                        <Box>
                          <Box
                            sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}
                            data-refer={`version-props-${index}`}
                          >
                            <Box>
                              <TextField
                                size='small'
                                name='key'
                                label='Key'
                                value={item.key}
                                disabled={item.disabled || (item.key === 'location' && type === 'update')}
                                onChange={event => handleFormChange({ index, event })}
                                error={item.hasDuplicateKey || item.invalid || !item.key?.trim()}
                                data-refer={`props-key-${index}`}
                              />
                            </Box>
                            <Box>
                              <TextField
                                size='small'
                                name='value'
                                label='Value'
                                error={item.required && item.value === ''}
                                value={item.value}
                                disabled={item.disabled || (item.key === 'location' && type === 'update')}
                                onChange={event => handleFormChange({ index, event })}
                                data-refer={`props-value-${index}`}
                                data-prev-refer={`props-${item.key}`}
                              />
                            </Box>

                            {!(item.disabled || (item.key === 'location' && type === 'update')) ? (
                              <Box sx={{ minWidth: 40 }}>
                                <IconButton onClick={() => removeFields(index)}>
                                  <Icon icon='mdi:minus-circle-outline' />
                                </IconButton>
                              </Box>
                            ) : (
                              <Box sx={{ minWidth: 40 }}></Box>
                            )}
                          </Box>
                        </Box>
                        <FormHelperText
                          sx={{
                            color: item.required && item.value === '' ? 'error.main' : 'text.main',
                            maxWidth: 'calc(100% - 40px)'
                          }}
                        >
                          {item.description}
                        </FormHelperText>
                        {item.hasDuplicateKey && (
                          <FormHelperText className={'twc-text-error-main'}>Key already exists</FormHelperText>
                        )}
                        {item.key && item.invalid && (
                          <FormHelperText className={'twc-text-error-main'}>
                            Valid key must starts with a letter/underscore, followed by alphanumeric characters,
                            underscores, hyphens, or dots.
                          </FormHelperText>
                        )}
                        {!item.key?.trim() && (
                          <FormHelperText className={'twc-text-error-main'}>Key is required</FormHelperText>
                        )}
                      </FormControl>
                    </Grid>
                  </Fragment>
                )
              })}
            </Grid>

            <Grid item xs={12}>
              <Button
                size='small'
                onClick={addFields}
                variant='outlined'
                startIcon={<Icon icon='mdi:plus-circle-outline' />}
                data-refer='add-version-props'
              >
                Add Property
              </Button>
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions
          sx={{
            justifyContent: 'center',
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pb: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <Button variant='contained' sx={{ mr: 1 }} type='submit' data-refer='handle-submit-model'>
            {type === 'create' ? 'Submit' : 'Update'}
          </Button>
          <Button variant='outlined' onClick={handleClose}>
            Cancel
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  )
}

export default LinkVersionDialog
