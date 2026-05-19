"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var _webmcp = require("./webmcp.js");
Object.keys(_webmcp).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _webmcp[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _webmcp[key];
    }
  });
});
var _nosqlQuerySchema = require("./nosql-query-schema.js");
Object.keys(_nosqlQuerySchema).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _nosqlQuerySchema[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _nosqlQuerySchema[key];
    }
  });
});
//# sourceMappingURL=index.js.map