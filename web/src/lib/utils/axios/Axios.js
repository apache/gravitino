/*
MIT License

Copyright (c) 2020-present, Vben

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
import axios from 'axios';
import qs from 'qs';
import { AxiosCanceler } from './axiosCancel';
import { isFunction } from '../../../../jsdist/src/lib/utils/is';
import { cloneDeep } from 'lodash-es';
import { ContentTypeEnum, RequestEnum } from '../../enums/httpEnum';
export * from './axiosTransform';
/**
 * @description: axios module
 */
var NextAxios = /** @class */ (function () {
    function NextAxios(options) {
        this.options = options;
        this.axiosInstance = axios.create(options);
        this.setupInterceptors();
    }
    /**
     * @description: Create axios instance
     */
    NextAxios.prototype.createAxios = function (config) {
        this.axiosInstance = axios.create(config);
    };
    NextAxios.prototype.getTransform = function () {
        var transform = this.options.transform;
        return transform;
    };
    NextAxios.prototype.getAxios = function () {
        return this.axiosInstance;
    };
    /**
     * @description: Reconfigure axios
     */
    NextAxios.prototype.configAxios = function (config) {
        if (!this.axiosInstance) {
            return;
        }
        this.createAxios(config);
    };
    /**
     * @description: Set general header
     */
    NextAxios.prototype.setHeader = function (headers) {
        if (!this.axiosInstance) {
            return;
        }
        Object.assign(this.axiosInstance.defaults.headers, headers);
    };
    /**
     * @description: Interceptor configuration
     */
    NextAxios.prototype.setupInterceptors = function () {
        var _this = this;
        var _a = this, axiosInstance = _a.axiosInstance, transform = _a.options.transform;
        if (!transform) {
            return;
        }
        var requestInterceptors = transform.requestInterceptors, requestInterceptorsCatch = transform.requestInterceptorsCatch, responseInterceptors = transform.responseInterceptors, responseInterceptorsCatch = transform.responseInterceptorsCatch;
        var axiosCanceler = new AxiosCanceler();
        // ** Request interceptor configuration processing
        this.axiosInstance.interceptors.request.use(function (config) {
            var _a, _b;
            // ** If cancel repeat request is turned on, then cancel repeat request is prohibited
            var requestOptions = (_a = config.requestOptions) !== null && _a !== void 0 ? _a : _this.options.requestOptions;
            var ignoreCancelToken = (_b = requestOptions === null || requestOptions === void 0 ? void 0 : requestOptions.ignoreCancelToken) !== null && _b !== void 0 ? _b : true;
            !ignoreCancelToken && axiosCanceler.addPending(config);
            if (requestInterceptors && isFunction(requestInterceptors)) {
                config = requestInterceptors(config, _this.options);
            }
            return config;
        }, undefined);
        // ** Request interceptor error capture
        requestInterceptorsCatch &&
            isFunction(requestInterceptorsCatch) &&
            this.axiosInstance.interceptors.request.use(undefined, requestInterceptorsCatch);
        // ** Response result interceptor processing
        this.axiosInstance.interceptors.response.use(function (res) {
            res && axiosCanceler.removePending(res.config);
            if (responseInterceptors && isFunction(responseInterceptors)) {
                res = responseInterceptors(res);
            }
            return res;
        }, undefined);
        // ** Response result interceptor error capture
        responseInterceptorsCatch &&
            isFunction(responseInterceptorsCatch) &&
            this.axiosInstance.interceptors.response.use(undefined, function (error) {
                return responseInterceptorsCatch(axiosInstance, error);
            });
    };
    /**
     * @description: File Upload
     */
    NextAxios.prototype.uploadFile = function (config, params) {
        var formData = new window.FormData();
        var customFilename = params.name || 'file';
        if (params.filename) {
            formData.append(customFilename, params.file, params.filename);
        }
        else {
            formData.append(customFilename, params.file);
        }
        if (params.data) {
            Object.keys(params.data).forEach(function (key) {
                var value = params.data[key];
                if (Array.isArray(value)) {
                    value.forEach(function (item) {
                        formData.append("".concat(key, "[]"), item);
                    });
                    return;
                }
                formData.append(key, params.data[key]);
            });
        }
        return this.axiosInstance.request(__assign(__assign({}, config), { method: 'POST', data: formData, headers: {
                'Content-type': ContentTypeEnum.FORM_DATA,
                ignoreCancelToken: true
            } }));
    };
    // ** support form-data
    NextAxios.prototype.supportFormData = function (config) {
        var _a;
        var headers = config.headers || this.options.headers;
        var contentType = (headers === null || headers === void 0 ? void 0 : headers['Content-Type']) || (headers === null || headers === void 0 ? void 0 : headers['content-type']);
        if (contentType !== ContentTypeEnum.FORM_URLENCODED ||
            !Reflect.has(config, 'data') ||
            ((_a = config.method) === null || _a === void 0 ? void 0 : _a.toUpperCase()) === RequestEnum.GET) {
            return config;
        }
        return __assign(__assign({}, config), { data: qs.stringify(config.data, { arrayFormat: 'brackets' }) });
    };
    NextAxios.prototype.get = function (config, options) {
        return this.request(__assign(__assign({}, config), { method: 'GET' }), options);
    };
    NextAxios.prototype.post = function (config, options) {
        return this.request(__assign(__assign({}, config), { method: 'POST' }), options);
    };
    NextAxios.prototype.put = function (config, options) {
        return this.request(__assign(__assign({}, config), { method: 'PUT' }), options);
    };
    NextAxios.prototype.delete = function (config, options) {
        return this.request(__assign(__assign({}, config), { method: 'DELETE' }), options);
    };
    NextAxios.prototype.request = function (config, options) {
        var _this = this;
        var conf = cloneDeep(config);
        if (config.cancelToken) {
            conf.cancelToken = config.cancelToken;
        }
        if (config.signal) {
            conf.signal = config.signal;
        }
        var transform = this.getTransform();
        var requestOptions = this.options.requestOptions;
        var opt = Object.assign({}, requestOptions, options);
        var _a = transform || {}, beforeRequestHook = _a.beforeRequestHook, requestCatchHook = _a.requestCatchHook, transformResponseHook = _a.transformResponseHook;
        if (beforeRequestHook && isFunction(beforeRequestHook)) {
            conf = beforeRequestHook(conf, opt);
        }
        conf.requestOptions = opt;
        conf = this.supportFormData(conf);
        return new Promise(function (resolve, reject) {
            _this.axiosInstance
                .request(conf)
                .then(function (res) {
                if (transformResponseHook && isFunction(transformResponseHook)) {
                    try {
                        var ret = transformResponseHook(res, opt);
                        resolve(ret);
                    }
                    catch (err) {
                        reject(err || new Error('request error!'));
                    }
                    return;
                }
                resolve(res);
            })
                .catch(function (e) {
                if (requestCatchHook && isFunction(requestCatchHook)) {
                    reject(requestCatchHook(e, opt));
                    return;
                }
                if (axios.isAxiosError(e)) {
                    // ** rewrite error message from axios in here
                }
                reject(e);
            });
        });
    };
    return NextAxios;
}());
export { NextAxios };
