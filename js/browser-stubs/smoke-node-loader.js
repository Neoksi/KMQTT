'use strict';

// Smoke-тест браузерной загрузки форка KMQTT без настоящего браузера.
// Раннера браузера в проекте нет, поэтому проверка имитирует окружение браузера поверх Node.js:
// 1) перехватываем require('node:*') и подставляем заглушки из stubs/ (то, что в браузере делает
//    webpack resolve.fallback из webpack.config.js);
// 2) прячем process.versions.node (в браузере process нет), чтобы фабрика MqttClient выбирала
//    браузерный хост;
// 3) грузим скомпилированный модуль kmqtt-client — его UMD-заголовок требует node:fs/crypto/tls/net
//    при загрузке, поэтому без заглушек в браузере он не поднялся бы;
// 4) проверяем, что заглушка бросает понятную ошибку при попытке использовать Node API.
//
// Запуск (node — тот, что Gradle скачивает для jsNodeTest, или любой Node.js 18+):
//   node js/browser-stubs/smoke-node-loader.js \
//       kmqtt-client/build/compileSync/js/test/testDevelopmentExecutable/kotlin
// Аргумент — каталог помодульных JS-файлов исполняемого бандла (берётся kmqtt-kmqtt-client.js) или один JS-файл.
//
// Выход: 0 и «SMOKE OK» при успехе, 1 при провале.

const fs = require('fs');
const Module = require('module');
const path = require('path');

let entryPath = process.argv[2];
if (!entryPath) {
    console.error('usage: node smoke-node-loader.js <каталог-или-файл-скомпилированного-бандла>');
    process.exit(1);
}
entryPath = path.resolve(entryPath);
if (fs.statSync(entryPath).isDirectory()) {
    entryPath = path.join(entryPath, 'kmqtt-kmqtt-client.js');
}
if (!fs.existsSync(entryPath)) {
    console.error('FAIL: не найден файл бандла: ' + entryPath);
    process.exit(1);
}

const stubDir = path.resolve(__dirname, 'stubs');
const stubFor = (request) => {
    const name = request.replace(/^node:/, '');
    return path.join(stubDir, 'node-' + name + '.js');
};

// Перехват идёт через Module.prototype.require: в современных Node внутренние require модулей
// не всегда проходят через Module._load
const originalRequire = Module.prototype.require;
const loadedStubs = [];
Module.prototype.require = function (request) {
    if (typeof request === 'string' && request.indexOf('node:') === 0 &&
        fs.existsSync(stubFor(request))) {
        loadedStubs.push(request);
        return originalRequire.call(this, stubFor(request));
    }
    return originalRequire.apply(this, arguments);
};

// Браузерный режим: у isNodeJs() форки не должно быть process.versions.node
let hiddenNode = false;
try {
    hiddenNode = delete process.versions.node;
} catch (e) {
    hiddenNode = false;
}
if (!hiddenNode) {
    console.log('WARN: process.versions.node не удалось скрыть');
}

try {
    require(entryPath);
} catch (e) {
    console.error('FAIL: бандл не загрузился с заглушками node:*: ' + (e && e.stack ? e.stack : e));
    process.exit(1);
}
console.log('OK: бандл загружен; перехвачены запросы: ' + (loadedStubs.join(', ') || '(нет)'));
if (loadedStubs.length === 0) {
    console.error('FAIL: бандл загрузился, но ни одного require(node:*) не перехвачено — проверка ничего не доказывает');
    process.exit(1);
}

// Заглушка должна бросать понятную ошибку при попытке использовать API Node
let stubError = null;
try {
    originalRequire.call(module, stubFor('node:net')).connect();
} catch (e) {
    stubError = e && e.message ? String(e.message) : String(e);
}
if (!stubError || stubError.indexOf('BrowserMqttClient') === -1) {
    console.error('FAIL: заглушка node:net не бросает ожидаемую ошибку: ' + stubError);
    process.exit(1);
}
console.log('OK: заглушка бросает ошибку при вызове: ' + stubError);

console.log('SMOKE OK');
