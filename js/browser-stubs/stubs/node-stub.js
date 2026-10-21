'use strict';

// Универсальная заглушка модуля node:* для браузерной сборки форка KMQTT.
// Любое обращение к свойству возвращает новую заглушку, поэтому библиотека загружается в браузере целиком
// (наследование классов, ссылки на конструкторы). Вызов или конструирование заглушки бросает понятную ошибку:
// сырые сокеты и TLS в браузере недоступны, нужен BrowserMqttClient (только WebSocket).

function makeNodeStub(name) {
    const target = function () {};
    return new Proxy(target, {
        get: function (t, prop) {
            if (prop === '__esModule') return true;
            if (prop === 'default') return t;
            if (prop === Symbol.toPrimitive) return function () { return '[browser stub ' + name + ']'; };
            if (prop === 'prototype') return t.prototype !== undefined ? t.prototype : (t.prototype = {});
            return makeNodeStub(name + '.' + String(prop));
        },
        set: function (t, prop, value) {
            t[prop] = value;
            return true;
        },
        apply: function () {
            throw new Error(
                name + ' is a browser stub of a Node.js module; raw sockets, TLS and the file system are not ' +
                    'available in a browser. Use BrowserMqttClient (MQTT over WebSocket only).'
            );
        },
        construct: function () {
            throw new Error(
                name + ' is a browser stub of a Node.js module; raw sockets, TLS and the file system are not ' +
                    'available in a browser. Use BrowserMqttClient (MQTT over WebSocket only).'
            );
        },
    });
}

module.exports = makeNodeStub;
