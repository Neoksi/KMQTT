# Браузерная сборка: заглушки `node:*`

Старый код `kmqtt-common` и `kmqtt-client` (legacy-клиент, Node-хост нового клиента) требует модули
`node:net`, `node:tls`, `node:fs`, `node:crypto` при загрузке модуля. В браузере их нет, поэтому бандл
без заглушек не поднимается. Сам браузерный хост (`BrowserMqttClient`, только WebSocket) API Node не зовёт —
заглушки нужны только чтобы модуль загрузился.

## Состав

- `stubs/node-stub.js` — универсальная заглушка: обращение к любому свойству возвращает новую заглушку
  (библиотека загружается целиком, включая наследование классов), вызов или конструирование бросает
  понятную ошибку «используйте BrowserMqttClient».
- `stubs/node-{net,tls,fs,crypto,events,buffer}.js` — заглушки конкретных модулей.
- `webpack.config.js` — готовый фрагмент `resolve.fallback` для webpack 5 (ключи с префиксом `node:` и без).
  Объедините со своей конфигурацией (см. комментарий в файле).
- `smoke-node-loader.js` — проверка загрузки без настоящего браузера.

## Как проверить загрузку (без браузера)

Раннера браузера в форке нет (JS-таргет собирает только `nodejs`; Karma/Phantom не подключены), поэтому
браузерный тест честно пропущен, а вместо него — smoke-тест, имитирующий браузер поверх Node.js: перехват
`require('node:*')` на заглушки (то, что в браузере делает webpack `resolve.fallback`) и скрытие
`process.versions.node` (в браузере `process` нет).

Скомпилируйте тестовый бандл (в нём живут Node-хост и его транспорты — именно они тянут `node:*`) и
прогоните (node — любой 18+, либо тот, что Gradle скачивает для `jsNodeTest`):

```bash
./gradlew :kmqtt-client:compileTestDevelopmentExecutableKotlinJs
node js/browser-stubs/smoke-node-loader.js \
    kmqtt-client/build/compileSync/js/test/testDevelopmentExecutable/kotlin
```

Ожидается `SMOKE OK`: бандл загрузился с заглушками (перехвачены `node:fs`, `node:crypto`, `node:tls`,
`node:net` из его UMD-заголовка), а вызов API из заглушки бросает понятную ошибку про `BrowserMqttClient`.
Примечание: экспорты исполняемого бандла переманглены (это не библиотечный артефакт), поэтому smoke
проверяет именно загрузку — то, что в браузере и было сломано.
