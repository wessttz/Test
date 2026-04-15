'use strict';

const { EvoDB, EvoConnection } = require('./src/client');

module.exports = { EvoDB, EvoConnection };

// Allow: const db = require('evodb').default  (ESM compat)
module.exports.default = { EvoDB, EvoConnection };
