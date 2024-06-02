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
/**
 * Referred from src/utils/http/axios/helper.ts
 */
import { isObject, isString } from '@/lib/utils/is';
var DATE_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss';
export function joinTimestamp(join, restful) {
    if (restful === void 0) { restful = false; }
    if (!join) {
        return restful ? '' : {};
    }
    var now = new Date().getTime();
    if (restful) {
        return "?_t=".concat(now);
    }
    return { _t: now };
}
/**
 * @description: Format request parameter time
 */
export function formatRequestDate(params) {
    var _a, _b;
    if (Object.prototype.toString.call(params) !== '[object Object]') {
        return;
    }
    for (var key in params) {
        var format = (_b = (_a = params[key]) === null || _a === void 0 ? void 0 : _a.format) !== null && _b !== void 0 ? _b : null;
        if (format && typeof format === 'function') {
            params[key] = params[key].format(DATE_TIME_FORMAT);
        }
        if (isString(key)) {
            var value = params[key];
            if (value) {
                try {
                    params[key] = isString(value) ? value.trim() : value;
                }
                catch (error) {
                    throw new Error(error);
                }
            }
        }
        if (isObject(params[key])) {
            formatRequestDate(params[key]);
        }
    }
}
