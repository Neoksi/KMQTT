'use strict';

// Готовая конфигурация заглушек node:* для браузерной сборки форка KMQTT.
// Старый код kmqtt-common/kmqtt-client требует модули node:* при загрузке, поэтому без заглушек бандл
// в браузере не поднимется. Ключи с префиксом node: и без него — webpack по-разному запрашивает
// встроенные модули в зависимости от версии и от того, как выставлен experiment.
//
// Использование: объедините resolve.fallback ниже со своей конфигурацией (webpack 5):
//   const kmqttBrowser = require('./js/browser-stubs/webpack.config.js');
//   module.exports = { ...ваша конфигурация..., resolve: { fallback: { ...kmqttBrowser.resolve.fallback } } };

const path = require('path');

const stub = (name) => path.resolve(__dirname, 'stubs', name);

const fallback = {
    net: stub('node-net.js'),
    'node:net': stub('node-net.js'),
    tls: stub('node-tls.js'),
    'node:tls': stub('node-tls.js'),
    fs: stub('node-fs.js'),
    'node:fs': stub('node-fs.js'),
    crypto: stub('node-crypto.js'),
    'node:crypto': stub('node-crypto.js'),
    events: stub('node-events.js'),
    'node:events': stub('node-events.js'),
    buffer: stub('node-buffer.js'),
    'node:buffer': stub('node-buffer.js'),
};

module.exports = {
    resolve: { fallback: fallback },
};
