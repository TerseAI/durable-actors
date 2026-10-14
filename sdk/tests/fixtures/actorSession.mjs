var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __knownSymbol = (name, symbol) => (symbol = Symbol[name]) ? symbol : /* @__PURE__ */ Symbol.for("Symbol." + name);
var __typeError = (msg) => {
  throw TypeError(msg);
};
var __defNormalProp = (obj, key, value) => key in obj ? __defProp(obj, key, { enumerable: true, configurable: true, writable: true, value }) : obj[key] = value;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __decoratorStart = (base) => [, , , __create(base?.[__knownSymbol("metadata")] ?? null)];
var __decoratorStrings = ["class", "method", "getter", "setter", "accessor", "field", "value", "get", "set"];
var __expectFn = (fn) => fn !== void 0 && typeof fn !== "function" ? __typeError("Function expected") : fn;
var __decoratorContext = (kind, name, done, metadata, fns) => ({ kind: __decoratorStrings[kind], name, metadata, addInitializer: (fn) => done._ ? __typeError("Already initialized") : fns.push(__expectFn(fn || null)) });
var __decoratorMetadata = (array, target) => __defNormalProp(target, __knownSymbol("metadata"), array[3]);
var __runInitializers = (array, flags, self, value) => {
  for (var i = 0, fns = array[flags >> 1], n = fns && fns.length; i < n; i++) flags & 1 ? fns[i].call(self) : value = fns[i].call(self, value);
  return value;
};
var __decorateElement = (array, flags, name, decorators, target, extra) => {
  var fn, it, done, ctx, access, k = flags & 7, s = !!(flags & 8), p = !!(flags & 16);
  var j = k > 3 ? array.length + 1 : k ? s ? 1 : 2 : 0, key = __decoratorStrings[k + 5];
  var initializers = k > 3 && (array[j - 1] = []), extraInitializers = array[j] || (array[j] = []);
  var desc = k && (!p && !s && (target = target.prototype), k < 5 && (k > 3 || !p) && __getOwnPropDesc(k < 4 ? target : { get [name]() {
    return __privateGet(this, extra);
  }, set [name](x) {
    return __privateSet(this, extra, x);
  } }, name));
  k ? p && k < 4 && __name(extra, (k > 2 ? "set " : k > 1 ? "get " : "") + name) : __name(target, name);
  for (var i = decorators.length - 1; i >= 0; i--) {
    ctx = __decoratorContext(k, name, done = {}, array[3], extraInitializers);
    if (k) {
      ctx.static = s, ctx.private = p, access = ctx.access = { has: p ? (x) => __privateIn(target, x) : (x) => name in x };
      if (k ^ 3) access.get = p ? (x) => (k ^ 1 ? __privateGet : __privateMethod)(x, target, k ^ 4 ? extra : desc.get) : (x) => x[name];
      if (k > 2) access.set = p ? (x, y) => __privateSet(x, target, y, k ^ 4 ? extra : desc.set) : (x, y) => x[name] = y;
    }
    it = (0, decorators[i])(k ? k < 4 ? p ? extra : desc[key] : k > 4 ? void 0 : { get: desc.get, set: desc.set } : target, ctx), done._ = 1;
    if (k ^ 4 || it === void 0) __expectFn(it) && (k > 4 ? initializers.unshift(it) : k ? p ? extra = it : desc[key] = it : target = it);
    else if (typeof it !== "object" || it === null) __typeError("Object expected");
    else __expectFn(fn = it.get) && (desc.get = fn), __expectFn(fn = it.set) && (desc.set = fn), __expectFn(fn = it.init) && initializers.unshift(fn);
  }
  return k || __decoratorMetadata(array, target), desc && __defProp(target, name, desc), p ? k ^ 4 ? extra : desc : target;
};
var __publicField = (obj, key, value) => __defNormalProp(obj, typeof key !== "symbol" ? key + "" : key, value);
var __accessCheck = (obj, member, msg) => member.has(obj) || __typeError("Cannot " + msg);
var __privateIn = (member, obj) => Object(obj) !== obj ? __typeError('Cannot use the "in" operator on this value') : member.has(obj);
var __privateGet = (obj, member, getter) => (__accessCheck(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateSet = (obj, member, value, setter) => (__accessCheck(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod = (obj, member, method) => (__accessCheck(obj, member, "access private method"), method);

// sdk/tests/fixtures/actorSession.ts
var actorSession_exports = {};
__export(actorSession_exports, {
  SessionCounter: () => SessionCounter
});
import { Actor, Atomic, Persisted } from "/Users/olimorissette/Desktop/desktop/projects/durable-objects/sdk/dist/index.js";
var _failAtomic_dec, _incrementAtomic_dec, _count_dec, _a, _init;
var _SessionCounter = class _SessionCounter extends (_a = Actor, _count_dec = [Persisted], _incrementAtomic_dec = [Atomic], _failAtomic_dec = [Atomic], _a) {
  constructor() {
    super(...arguments);
    __runInitializers(_init, 5, this);
    __publicField(this, "count", __runInitializers(_init, 8, this, 0)), __runInitializers(_init, 11, this);
  }
  async increment(amount = 1) {
    this.count += amount;
    return this.count;
  }
  async incrementAtomic() {
    return this.incrementTwice();
  }
  async incrementTwice() {
    this.count++;
    this.count++;
    return this.count;
  }
  async failAfterWrite() {
    this.count++;
    throw new Error("failed after write");
  }
  async failAtomic() {
    return this.failAfterWrite();
  }
  async read() {
    return this.count;
  }
  async stream() {
    this.broadcast({ delta: "first" });
    this.broadcast({ delta: "last" });
    return this.count;
  }
  async announceThenSpin() {
    await _SessionCounter.get("worker-start-observer").increment(0);
    return this.spinForever();
  }
  async spinForever() {
    while (true) {
      void performance.now();
    }
  }
  async onAlarm() {
    this.count++;
  }
};
_init = __decoratorStart(_a);
__decorateElement(_init, 1, "incrementAtomic", _incrementAtomic_dec, _SessionCounter);
__decorateElement(_init, 1, "failAtomic", _failAtomic_dec, _SessionCounter);
__decorateElement(_init, 5, "count", _count_dec, _SessionCounter);
__decoratorMetadata(_init, _SessionCounter);
__name(_SessionCounter, "SessionCounter");
var SessionCounter = _SessionCounter;

// sdk/tests/fixtures/actor-artifact.ts
var schemas = [{ "actorName": "SessionCounter", "fields": [{ "name": "count", "persistence": "persisted", "private": false, "visibility": "private", "emittable": false, "resource": false }], "atomicMethods": ["incrementAtomic", "failAtomic"], "contract": { "version": 1, "actorName": "SessionCounter", "schema": { "$schema": "http://json-schema.org/draft-07/schema#", "definitions": { "Metadata": { "anyOf": [{ "$ref": "#/definitions/Type:1" }, { "$ref": "#/definitions/Type:2" }, { "$ref": "#/definitions/Type:3" }, { "$ref": "#/definitions/Type:4" }, { "$ref": "#/definitions/Type:5" }, { "$ref": "#/definitions/Type:6" }, { "$ref": "#/definitions/Type:7" }], "title": "JsonValue" }, "Incoming": { "anyOf": [{ "$ref": "#/definitions/Type:1" }, { "$ref": "#/definitions/Type:2" }, { "$ref": "#/definitions/Type:3" }, { "$ref": "#/definitions/Type:4" }, { "$ref": "#/definitions/Type:5" }, { "$ref": "#/definitions/Type:6" }, { "$ref": "#/definitions/Type:7" }], "title": "JsonValue" }, "Outgoing": { "anyOf": [{ "$ref": "#/definitions/Type:1" }, { "$ref": "#/definitions/Type:2" }, { "$ref": "#/definitions/Type:3" }, { "$ref": "#/definitions/Type:4" }, { "$ref": "#/definitions/Type:5" }, { "$ref": "#/definitions/Type:6" }, { "$ref": "#/definitions/Type:7" }], "title": "JsonValue" }, "Type:1": { "type": "null" }, "Type:2": { "type": "string" }, "Type:3": { "type": "number" }, "Type:4": { "type": "boolean", "const": false }, "Type:5": { "type": "boolean", "const": true }, "Type:6": { "$ref": "#/definitions/import(%22%2FUsers%2Folimorissette%2FDesktop%2Fdesktop%2Fprojects%2Fdurable-objects%2Fsdk%2Fdist%2Fjson%22%2C%7Bwith%3A%7B%22resolution-mode%22%3A%22import%22%7D%7D).JsonObject" }, "Type:7": { "type": "array", "items": { "anyOf": [{ "$ref": "#/definitions/Type:1" }, { "$ref": "#/definitions/Type:2" }, { "$ref": "#/definitions/Type:3" }, { "$ref": "#/definitions/Type:4" }, { "$ref": "#/definitions/Type:5" }, { "$ref": "#/definitions/Type:6" }, { "$ref": "#/definitions/Type:7" }], "title": "JsonValue" } }, 'import("/Users/olimorissette/Desktop/desktop/projects/durable-objects/sdk/dist/json",{with:{"resolution-mode":"import"}}).JsonValue': { "anyOf": [{ "$ref": "#/definitions/import(%22%2FUsers%2Folimorissette%2FDesktop%2Fdesktop%2Fprojects%2Fdurable-objects%2Fsdk%2Fdist%2Fjson%22%2C%7Bwith%3A%7B%22resolution-mode%22%3A%22import%22%7D%7D).JsonObject" }, { "type": "array", "items": { "$ref": "#/definitions/import(%22%2FUsers%2Folimorissette%2FDesktop%2Fdesktop%2Fprojects%2Fdurable-objects%2Fsdk%2Fdist%2Fjson%22%2C%7Bwith%3A%7B%22resolution-mode%22%3A%22import%22%7D%7D).JsonValue" } }, { "type": ["null", "string", "number", "boolean"] }] }, 'import("/Users/olimorissette/Desktop/desktop/projects/durable-objects/sdk/dist/json",{with:{"resolution-mode":"import"}}).JsonObject': { "type": "object", "additionalProperties": { "$ref": "#/definitions/Outgoing" }, "title": "JsonObject" }, "State": { "type": "object", "properties": {}, "required": [] } } }, "emittable": [] } }];
var version = 1;
export {
  actorSession_exports as actors,
  schemas,
  version
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsiYWN0b3JTZXNzaW9uLnRzIiwgImFjdG9yLWFydGlmYWN0LnRzIl0sCiAgInNvdXJjZXNDb250ZW50IjogWyJpbXBvcnQgeyBBY3RvciwgQXRvbWljLCBQZXJzaXN0ZWQgfSBmcm9tIFwiZHVyYWJsZS1hY3RvcnNcIlxuXG5jbGFzcyBTZXNzaW9uQ291bnRlciBleHRlbmRzIEFjdG9yIHtcbiAgICBAUGVyc2lzdGVkIHByaXZhdGUgY291bnQgPSAwXG5cbiAgICBhc3luYyBpbmNyZW1lbnQoYW1vdW50ID0gMSk6IFByb21pc2U8bnVtYmVyPiB7XG4gICAgICAgIHRoaXMuY291bnQgKz0gYW1vdW50XG4gICAgICAgIHJldHVybiB0aGlzLmNvdW50XG4gICAgfVxuXG4gICAgQEF0b21pY1xuICAgIGFzeW5jIGluY3JlbWVudEF0b21pYygpOiBQcm9taXNlPG51bWJlcj4ge1xuICAgICAgICByZXR1cm4gdGhpcy5pbmNyZW1lbnRUd2ljZSgpXG4gICAgfVxuXG4gICAgYXN5bmMgaW5jcmVtZW50VHdpY2UoKTogUHJvbWlzZTxudW1iZXI+IHtcbiAgICAgICAgdGhpcy5jb3VudCsrXG4gICAgICAgIHRoaXMuY291bnQrK1xuICAgICAgICByZXR1cm4gdGhpcy5jb3VudFxuICAgIH1cblxuICAgIGFzeW5jIGZhaWxBZnRlcldyaXRlKCk6IFByb21pc2U8dm9pZD4ge1xuICAgICAgICB0aGlzLmNvdW50KytcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKFwiZmFpbGVkIGFmdGVyIHdyaXRlXCIpXG4gICAgfVxuXG4gICAgQEF0b21pY1xuICAgIGFzeW5jIGZhaWxBdG9taWMoKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgICAgIHJldHVybiB0aGlzLmZhaWxBZnRlcldyaXRlKClcbiAgICB9XG5cbiAgICBhc3luYyByZWFkKCk6IFByb21pc2U8bnVtYmVyPiB7XG4gICAgICAgIHJldHVybiB0aGlzLmNvdW50XG4gICAgfVxuXG4gICAgYXN5bmMgc3RyZWFtKCk6IFByb21pc2U8bnVtYmVyPiB7XG4gICAgICAgIHRoaXMuYnJvYWRjYXN0KHsgZGVsdGE6IFwiZmlyc3RcIiB9KVxuICAgICAgICB0aGlzLmJyb2FkY2FzdCh7IGRlbHRhOiBcImxhc3RcIiB9KVxuICAgICAgICByZXR1cm4gdGhpcy5jb3VudFxuICAgIH1cblxuICAgIGFzeW5jIGFubm91bmNlVGhlblNwaW4oKTogUHJvbWlzZTxuZXZlcj4ge1xuICAgICAgICBhd2FpdCBTZXNzaW9uQ291bnRlci5nZXQoXCJ3b3JrZXItc3RhcnQtb2JzZXJ2ZXJcIikuaW5jcmVtZW50KDApXG4gICAgICAgIHJldHVybiB0aGlzLnNwaW5Gb3JldmVyKClcbiAgICB9XG5cbiAgICBhc3luYyBzcGluRm9yZXZlcigpOiBQcm9taXNlPG5ldmVyPiB7XG4gICAgICAgIC8vIERlbGliZXJhdGVseSB1bmNvb3BlcmF0aXZlIGNvZGUgdXNlZCB0byB2ZXJpZnkgaGFyZCBXb3JrZXIgdGVybWluYXRpb24uXG4gICAgICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICAgICAgICAvLyBLZWVwIHRoZSBsb29wIG9wYXF1ZSB0byBvcHRpbWl6ZXJzIHdpdGhvdXQgeWllbGRpbmcgdGhlIFdvcmtlciBldmVudCBsb29wLlxuICAgICAgICAgICAgdm9pZCBwZXJmb3JtYW5jZS5ub3coKVxuICAgICAgICB9XG4gICAgfVxuXG4gICAgYXN5bmMgb25BbGFybSgpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICAgICAgdGhpcy5jb3VudCsrXG4gICAgfVxufVxuXG5leHBvcnQgeyBTZXNzaW9uQ291bnRlciB9XG4iLCAiaW1wb3J0ICogYXMgYWN0b3JzIGZyb20gXCIvVXNlcnMvb2xpbW9yaXNzZXR0ZS9EZXNrdG9wL2Rlc2t0b3AvcHJvamVjdHMvZHVyYWJsZS1vYmplY3RzL3Nkay90ZXN0cy9maXh0dXJlcy9hY3RvclNlc3Npb24udHNcIjtcbiAgICAgICAgICAgICAgICBleHBvcnQgeyBhY3RvcnMgfTtcbiAgICAgICAgICAgICAgICBleHBvcnQgY29uc3Qgc2NoZW1hcyA9IFt7XCJhY3Rvck5hbWVcIjpcIlNlc3Npb25Db3VudGVyXCIsXCJmaWVsZHNcIjpbe1wibmFtZVwiOlwiY291bnRcIixcInBlcnNpc3RlbmNlXCI6XCJwZXJzaXN0ZWRcIixcInByaXZhdGVcIjpmYWxzZSxcInZpc2liaWxpdHlcIjpcInByaXZhdGVcIixcImVtaXR0YWJsZVwiOmZhbHNlLFwicmVzb3VyY2VcIjpmYWxzZX1dLFwiYXRvbWljTWV0aG9kc1wiOltcImluY3JlbWVudEF0b21pY1wiLFwiZmFpbEF0b21pY1wiXSxcImNvbnRyYWN0XCI6e1widmVyc2lvblwiOjEsXCJhY3Rvck5hbWVcIjpcIlNlc3Npb25Db3VudGVyXCIsXCJzY2hlbWFcIjp7XCIkc2NoZW1hXCI6XCJodHRwOi8vanNvbi1zY2hlbWEub3JnL2RyYWZ0LTA3L3NjaGVtYSNcIixcImRlZmluaXRpb25zXCI6e1wiTWV0YWRhdGFcIjp7XCJhbnlPZlwiOlt7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6MVwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6MlwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6M1wifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NFwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NVwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NlwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6N1wifV0sXCJ0aXRsZVwiOlwiSnNvblZhbHVlXCJ9LFwiSW5jb21pbmdcIjp7XCJhbnlPZlwiOlt7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6MVwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6MlwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6M1wifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NFwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NVwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NlwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6N1wifV0sXCJ0aXRsZVwiOlwiSnNvblZhbHVlXCJ9LFwiT3V0Z29pbmdcIjp7XCJhbnlPZlwiOlt7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6MVwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6MlwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6M1wifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NFwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NVwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NlwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6N1wifV0sXCJ0aXRsZVwiOlwiSnNvblZhbHVlXCJ9LFwiVHlwZToxXCI6e1widHlwZVwiOlwibnVsbFwifSxcIlR5cGU6MlwiOntcInR5cGVcIjpcInN0cmluZ1wifSxcIlR5cGU6M1wiOntcInR5cGVcIjpcIm51bWJlclwifSxcIlR5cGU6NFwiOntcInR5cGVcIjpcImJvb2xlYW5cIixcImNvbnN0XCI6ZmFsc2V9LFwiVHlwZTo1XCI6e1widHlwZVwiOlwiYm9vbGVhblwiLFwiY29uc3RcIjp0cnVlfSxcIlR5cGU6NlwiOntcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvaW1wb3J0KCUyMiUyRlVzZXJzJTJGb2xpbW9yaXNzZXR0ZSUyRkRlc2t0b3AlMkZkZXNrdG9wJTJGcHJvamVjdHMlMkZkdXJhYmxlLW9iamVjdHMlMkZzZGslMkZkaXN0JTJGanNvbiUyMiUyQyU3QndpdGglM0ElN0IlMjJyZXNvbHV0aW9uLW1vZGUlMjIlM0ElMjJpbXBvcnQlMjIlN0QlN0QpLkpzb25PYmplY3RcIn0sXCJUeXBlOjdcIjp7XCJ0eXBlXCI6XCJhcnJheVwiLFwiaXRlbXNcIjp7XCJhbnlPZlwiOlt7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6MVwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6MlwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6M1wifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NFwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NVwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6NlwifSx7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL1R5cGU6N1wifV0sXCJ0aXRsZVwiOlwiSnNvblZhbHVlXCJ9fSxcImltcG9ydChcXFwiL1VzZXJzL29saW1vcmlzc2V0dGUvRGVza3RvcC9kZXNrdG9wL3Byb2plY3RzL2R1cmFibGUtb2JqZWN0cy9zZGsvZGlzdC9qc29uXFxcIix7d2l0aDp7XFxcInJlc29sdXRpb24tbW9kZVxcXCI6XFxcImltcG9ydFxcXCJ9fSkuSnNvblZhbHVlXCI6e1wiYW55T2ZcIjpbe1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9pbXBvcnQoJTIyJTJGVXNlcnMlMkZvbGltb3Jpc3NldHRlJTJGRGVza3RvcCUyRmRlc2t0b3AlMkZwcm9qZWN0cyUyRmR1cmFibGUtb2JqZWN0cyUyRnNkayUyRmRpc3QlMkZqc29uJTIyJTJDJTdCd2l0aCUzQSU3QiUyMnJlc29sdXRpb24tbW9kZSUyMiUzQSUyMmltcG9ydCUyMiU3RCU3RCkuSnNvbk9iamVjdFwifSx7XCJ0eXBlXCI6XCJhcnJheVwiLFwiaXRlbXNcIjp7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL2ltcG9ydCglMjIlMkZVc2VycyUyRm9saW1vcmlzc2V0dGUlMkZEZXNrdG9wJTJGZGVza3RvcCUyRnByb2plY3RzJTJGZHVyYWJsZS1vYmplY3RzJTJGc2RrJTJGZGlzdCUyRmpzb24lMjIlMkMlN0J3aXRoJTNBJTdCJTIycmVzb2x1dGlvbi1tb2RlJTIyJTNBJTIyaW1wb3J0JTIyJTdEJTdEKS5Kc29uVmFsdWVcIn19LHtcInR5cGVcIjpbXCJudWxsXCIsXCJzdHJpbmdcIixcIm51bWJlclwiLFwiYm9vbGVhblwiXX1dfSxcImltcG9ydChcXFwiL1VzZXJzL29saW1vcmlzc2V0dGUvRGVza3RvcC9kZXNrdG9wL3Byb2plY3RzL2R1cmFibGUtb2JqZWN0cy9zZGsvZGlzdC9qc29uXFxcIix7d2l0aDp7XFxcInJlc29sdXRpb24tbW9kZVxcXCI6XFxcImltcG9ydFxcXCJ9fSkuSnNvbk9iamVjdFwiOntcInR5cGVcIjpcIm9iamVjdFwiLFwiYWRkaXRpb25hbFByb3BlcnRpZXNcIjp7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL091dGdvaW5nXCJ9LFwidGl0bGVcIjpcIkpzb25PYmplY3RcIn0sXCJTdGF0ZVwiOntcInR5cGVcIjpcIm9iamVjdFwiLFwicHJvcGVydGllc1wiOnt9LFwicmVxdWlyZWRcIjpbXX19fSxcImVtaXR0YWJsZVwiOltdfX1dO1xuICAgICAgICAgICAgICAgIGV4cG9ydCBjb25zdCB2ZXJzaW9uID0gMTsiXSwKICAibWFwcGluZ3MiOiAiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFBQTtBQUFBO0FBQUE7QUFBQTtBQUFBLFNBQVMsT0FBTyxRQUFRLGlCQUFpQjtBQUF6QztBQUVBLElBQU0sa0JBQU4sTUFBTSx5QkFBdUIsWUFDekIsY0FBQyxZQU9ELHdCQUFDLFNBZ0JELG1CQUFDLFNBeEJ3QixJQUFNO0FBQUEsRUFBbkM7QUFBQTtBQUFBO0FBQ2Usd0JBQVEsU0FBUSxrQkFBaEIsZ0JBQWdCLEtBQWhCO0FBQUE7QUFBQSxFQUVYLE1BQU0sVUFBVSxTQUFTLEdBQW9CO0FBQ3pDLFNBQUssU0FBUztBQUNkLFdBQU8sS0FBSztBQUFBLEVBQ2hCO0FBQUEsRUFHQSxNQUFNLGtCQUFtQztBQUNyQyxXQUFPLEtBQUssZUFBZTtBQUFBLEVBQy9CO0FBQUEsRUFFQSxNQUFNLGlCQUFrQztBQUNwQyxTQUFLO0FBQ0wsU0FBSztBQUNMLFdBQU8sS0FBSztBQUFBLEVBQ2hCO0FBQUEsRUFFQSxNQUFNLGlCQUFnQztB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
