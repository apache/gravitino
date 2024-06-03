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
var pendingMap = new Map();
var getPendingUrl = function (config) {
    return [config.method, config.url].join('&');
};
var AxiosCanceler = /** @class */ (function () {
    function AxiosCanceler() {
    }
    AxiosCanceler.prototype.addPending = function (config) {
        this.removePending(config);
        var url = getPendingUrl(config);
        var controller = new AbortController();
        config.signal = config.signal || controller.signal;
        if (!pendingMap.has(url)) {
            pendingMap.set(url, controller);
        }
    };
    AxiosCanceler.prototype.removeAllPending = function () {
        pendingMap.forEach(function (abortController) {
            if (abortController) {
                abortController.abort();
            }
        });
        this.reset();
    };
    AxiosCanceler.prototype.removePending = function (config) {
        var url = getPendingUrl(config);
        if (pendingMap.has(url)) {
            var abortController = pendingMap.get(url);
            if (abortController) {
                abortController.abort(url);
            }
            pendingMap.delete(url);
        }
    };
    AxiosCanceler.prototype.reset = function () {
        pendingMap.clear();
    };
    return AxiosCanceler;
}());
export { AxiosCanceler };