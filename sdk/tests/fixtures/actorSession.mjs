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

// tests/fixtures/actorSession.ts
var actorSession_exports = {};
__export(actorSession_exports, {
  SessionCounter: () => SessionCounter
});
import { Actor, Persisted } from "/Users/oliviersimard-morissette/Documents/Github Projects/durable-actors/sdk/dist/index.js";
var _count_dec, _a, _init;
var _SessionCounter = class _SessionCounter extends (_a = Actor, _count_dec = [Persisted], _a) {
  constructor() {
    super(...arguments);
    __publicField(this, "count", __runInitializers(_init, 8, this, 0)), __runInitializers(_init, 11, this);
  }
  async increment(amount = 1) {
    this.count += amount;
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
};
_init = __decoratorStart(_a);
__decorateElement(_init, 5, "count", _count_dec, _SessionCounter);
__decoratorMetadata(_init, _SessionCounter);
__name(_SessionCounter, "SessionCounter");
var SessionCounter = _SessionCounter;

// tests/fixtures/actor-artifact.ts
var schemas = [{ "actorName": "SessionCounter", "fields": [{ "name": "count", "persistence": "persisted", "visibility": "private" }], "contract": { "version": 1, "actorName": "SessionCounter", "schema": { "$schema": "http://json-schema.org/draft-07/schema#", "definitions": { "Metadata": { "anyOf": [{ "$ref": "#/definitions/Type:1" }, { "$ref": "#/definitions/Type:2" }, { "$ref": "#/definitions/Type:3" }, { "$ref": "#/definitions/Type:4" }, { "$ref": "#/definitions/Type:5" }, { "$ref": "#/definitions/Type:6" }, { "$ref": "#/definitions/Type:7" }], "title": "JsonValue" }, "Incoming": { "anyOf": [{ "$ref": "#/definitions/Type:1" }, { "$ref": "#/definitions/Type:2" }, { "$ref": "#/definitions/Type:3" }, { "$ref": "#/definitions/Type:4" }, { "$ref": "#/definitions/Type:5" }, { "$ref": "#/definitions/Type:6" }, { "$ref": "#/definitions/Type:7" }], "title": "JsonValue" }, "Outgoing": { "anyOf": [{ "$ref": "#/definitions/Type:1" }, { "$ref": "#/definitions/Type:2" }, { "$ref": "#/definitions/Type:3" }, { "$ref": "#/definitions/Type:4" }, { "$ref": "#/definitions/Type:5" }, { "$ref": "#/definitions/Type:6" }, { "$ref": "#/definitions/Type:7" }], "title": "JsonValue" }, "Type:1": { "type": "null" }, "Type:2": { "type": "string" }, "Type:3": { "type": "number" }, "Type:4": { "type": "boolean", "const": false }, "Type:5": { "type": "boolean", "const": true }, "Type:6": { "$ref": "#/definitions/import(%22%2FUsers%2Foliviersimard-morissette%2FDocuments%2FGithubProjects%2Fdurable-actors%2Fsdk%2Fdist%2Fjson%22%2C%7Bwith%3A%7B%22resolution-mode%22%3A%22import%22%7D%7D).JsonObject" }, "Type:7": { "type": "array", "items": { "anyOf": [{ "$ref": "#/definitions/Type:1" }, { "$ref": "#/definitions/Type:2" }, { "$ref": "#/definitions/Type:3" }, { "$ref": "#/definitions/Type:4" }, { "$ref": "#/definitions/Type:5" }, { "$ref": "#/definitions/Type:6" }, { "$ref": "#/definitions/Type:7" }], "title": "JsonValue" } }, 'import("/Users/oliviersimard-morissette/Documents/GithubProjects/durable-actors/sdk/dist/json",{with:{"resolution-mode":"import"}}).JsonValue': { "anyOf": [{ "$ref": "#/definitions/import(%22%2FUsers%2Foliviersimard-morissette%2FDocuments%2FGithubProjects%2Fdurable-actors%2Fsdk%2Fdist%2Fjson%22%2C%7Bwith%3A%7B%22resolution-mode%22%3A%22import%22%7D%7D).JsonObject" }, { "type": "array", "items": { "$ref": "#/definitions/import(%22%2FUsers%2Foliviersimard-morissette%2FDocuments%2FGithubProjects%2Fdurable-actors%2Fsdk%2Fdist%2Fjson%22%2C%7Bwith%3A%7B%22resolution-mode%22%3A%22import%22%7D%7D).JsonValue" } }, { "type": ["null", "string", "number", "boolean"] }] }, 'import("/Users/oliviersimard-morissette/Documents/GithubProjects/durable-actors/sdk/dist/json",{with:{"resolution-mode":"import"}}).JsonObject': { "type": "object", "additionalProperties": { "$ref": "#/definitions/Outgoing" }, "title": "JsonObject" }, "State": { "type": "object", "properties": {}, "required": [] } } }, "emittable": [] } }];
var version = 1;
export {
  actorSession_exports as actors,
  schemas,
  version
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsiYWN0b3JTZXNzaW9uLnRzIiwgImFjdG9yLWFydGlmYWN0LnRzIl0sCiAgInNvdXJjZXNDb250ZW50IjogWyJpbXBvcnQgeyBBY3RvciwgUGVyc2lzdGVkIH0gZnJvbSBcImR1cmFibGUtYWN0b3JzXCJcblxuY2xhc3MgU2Vzc2lvbkNvdW50ZXIgZXh0ZW5kcyBBY3RvciB7XG4gICAgQFBlcnNpc3RlZCBwcml2YXRlIGNvdW50ID0gMFxuXG4gICAgYXN5bmMgaW5jcmVtZW50KGFtb3VudCA9IDEpOiBQcm9taXNlPG51bWJlcj4ge1xuICAgICAgICB0aGlzLmNvdW50ICs9IGFtb3VudFxuICAgICAgICByZXR1cm4gdGhpcy5jb3VudFxuICAgIH1cblxuICAgIGFzeW5jIHN0cmVhbSgpOiBQcm9taXNlPG51bWJlcj4ge1xuICAgICAgICB0aGlzLmJyb2FkY2FzdCh7IGRlbHRhOiBcImZpcnN0XCIgfSlcbiAgICAgICAgdGhpcy5icm9hZGNhc3QoeyBkZWx0YTogXCJsYXN0XCIgfSlcbiAgICAgICAgcmV0dXJuIHRoaXMuY291bnRcbiAgICB9XG5cbiAgICBhc3luYyBhbm5vdW5jZVRoZW5TcGluKCk6IFByb21pc2U8bmV2ZXI+IHtcbiAgICAgICAgYXdhaXQgU2Vzc2lvbkNvdW50ZXIuZ2V0KFwid29ya2VyLXN0YXJ0LW9ic2VydmVyXCIpLmluY3JlbWVudCgwKVxuICAgICAgICByZXR1cm4gdGhpcy5zcGluRm9yZXZlcigpXG4gICAgfVxuXG4gICAgYXN5bmMgc3BpbkZvcmV2ZXIoKTogUHJvbWlzZTxuZXZlcj4ge1xuICAgICAgICAvLyBEZWxpYmVyYXRlbHkgdW5jb29wZXJhdGl2ZSBjb2RlIHVzZWQgdG8gdmVyaWZ5IGhhcmQgV29ya2VyIHRlcm1pbmF0aW9uLlxuICAgICAgICB3aGlsZSAodHJ1ZSkge1xuICAgICAgICAgICAgLy8gS2VlcCB0aGUgbG9vcCBvcGFxdWUgdG8gb3B0aW1pemVycyB3aXRob3V0IHlpZWxkaW5nIHRoZSBXb3JrZXIgZXZlbnQgbG9vcC5cbiAgICAgICAgICAgIHZvaWQgcGVyZm9ybWFuY2Uubm93KClcbiAgICAgICAgfVxuICAgIH1cbn1cblxuZXhwb3J0IHsgU2Vzc2lvbkNvdW50ZXIgfVxuIiwgImltcG9ydCAqIGFzIGFjdG9ycyBmcm9tIFwiL1VzZXJzL29saXZpZXJzaW1hcmQtbW9yaXNzZXR0ZS9Eb2N1bWVudHMvR2l0aHViIFByb2plY3RzL2R1cmFibGUtYWN0b3JzL3Nkay90ZXN0cy9maXh0dXJlcy9hY3RvclNlc3Npb24udHNcIjtcbiAgICAgICAgICAgICAgICBleHBvcnQgeyBhY3RvcnMgfTtcbiAgICAgICAgICAgICAgICBleHBvcnQgY29uc3Qgc2NoZW1hcyA9IFt7XCJhY3Rvck5hbWVcIjpcIlNlc3Npb25Db3VudGVyXCIsXCJmaWVsZHNcIjpbe1wibmFtZVwiOlwiY291bnRcIixcInBlcnNpc3RlbmNlXCI6XCJwZXJzaXN0ZWRcIixcInZpc2liaWxpdHlcIjpcInByaXZhdGVcIn1dLFwiY29udHJhY3RcIjp7XCJ2ZXJzaW9uXCI6MSxcImFjdG9yTmFtZVwiOlwiU2Vzc2lvbkNvdW50ZXJcIixcInNjaGVtYVwiOntcIiRzY2hlbWFcIjpcImh0dHA6Ly9qc29uLXNjaGVtYS5vcmcvZHJhZnQtMDcvc2NoZW1hI1wiLFwiZGVmaW5pdGlvbnNcIjp7XCJNZXRhZGF0YVwiOntcImFueU9mXCI6W3tcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZToxXCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZToyXCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTozXCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo0XCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo1XCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo2XCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo3XCJ9XSxcInRpdGxlXCI6XCJKc29uVmFsdWVcIn0sXCJJbmNvbWluZ1wiOntcImFueU9mXCI6W3tcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZToxXCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZToyXCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTozXCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo0XCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo1XCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo2XCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo3XCJ9XSxcInRpdGxlXCI6XCJKc29uVmFsdWVcIn0sXCJPdXRnb2luZ1wiOntcImFueU9mXCI6W3tcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZToxXCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZToyXCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTozXCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo0XCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo1XCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo2XCJ9LHtcIiRyZWZcIjpcIiMvZGVmaW5pdGlvbnMvVHlwZTo3XCJ9XSxcInRpdGxlXCI6XCJKc29uVmFsdWVcIn0sXCJUeXBlOjFcIjp7XCJ0eXBlXCI6XCJudWxsXCJ9LFwiVHlwZToyXCI6e1widHlwZVwiOlwic3RyaW5nXCJ9LFwiVHlwZTozXCI6e1widHlwZVwiOlwibnVtYmVyXCJ9LFwiVHlwZTo0XCI6e1widHlwZVwiOlwiYm9vbGVhblwiLFwiY29uc3RcIjpmYWxzZX0sXCJUeXBlOjVcIjp7XCJ0eXBlXCI6XCJib29sZWFuXCIsXCJjb25zdFwiOnRydWV9LFwiVHlwZTo2XCI6e1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9pbXBvcnQoJTIyJTJGVXNlcnMlMkZvbGl2aWVyc2ltYXJkLW1vcmlzc2V0dGUlMkZEb2N1bWVudHMlMkZHaXRodWJQcm9qZWN0cyUyRmR1cmFibGUtYWN0b3JzJTJGc2RrJTJGZGlzdCUyRmpzb24lMjIlMkMlN0J3aXRoJTNBJTdCJTIycmVzb2x1dGlvbi1tb2RlJTIyJTNBJTIyaW1wb3J0JTIyJTdEJTdEKS5Kc29uT2JqZWN0XCJ9LFwiVHlwZTo3XCI6e1widHlwZVwiOlwiYXJyYXlcIixcIml0ZW1zXCI6e1wiYW55T2ZcIjpbe1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9UeXBlOjFcIn0se1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9UeXBlOjJcIn0se1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9UeXBlOjNcIn0se1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9UeXBlOjRcIn0se1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9UeXBlOjVcIn0se1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9UeXBlOjZcIn0se1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9UeXBlOjdcIn1dLFwidGl0bGVcIjpcIkpzb25WYWx1ZVwifX0sXCJpbXBvcnQoXFxcIi9Vc2Vycy9vbGl2aWVyc2ltYXJkLW1vcmlzc2V0dGUvRG9jdW1lbnRzL0dpdGh1YlByb2plY3RzL2R1cmFibGUtYWN0b3JzL3Nkay9kaXN0L2pzb25cXFwiLHt3aXRoOntcXFwicmVzb2x1dGlvbi1tb2RlXFxcIjpcXFwiaW1wb3J0XFxcIn19KS5Kc29uVmFsdWVcIjp7XCJhbnlPZlwiOlt7XCIkcmVmXCI6XCIjL2RlZmluaXRpb25zL2ltcG9ydCglMjIlMkZVc2VycyUyRm9saXZpZXJzaW1hcmQtbW9yaXNzZXR0ZSUyRkRvY3VtZW50cyUyRkdpdGh1YlByb2plY3RzJTJGZHVyYWJsZS1hY3RvcnMlMkZzZGslMkZkaXN0JTJGanNvbiUyMiUyQyU3QndpdGglM0ElN0IlMjJyZXNvbHV0aW9uLW1vZGUlMjIlM0ElMjJpbXBvcnQlMjIlN0QlN0QpLkpzb25PYmplY3RcIn0se1widHlwZVwiOlwiYXJyYXlcIixcIml0ZW1zXCI6e1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9pbXBvcnQoJTIyJTJGVXNlcnMlMkZvbGl2aWVyc2ltYXJkLW1vcmlzc2V0dGUlMkZEb2N1bWVudHMlMkZHaXRodWJQcm9qZWN0cyUyRmR1cmFibGUtYWN0b3JzJTJGc2RrJTJGZGlzdCUyRmpzb24lMjIlMkMlN0J3aXRoJTNBJTdCJTIycmVzb2x1dGlvbi1tb2RlJTIyJTNBJTIyaW1wb3J0JTIyJTdEJTdEKS5Kc29uVmFsdWVcIn19LHtcInR5cGVcIjpbXCJudWxsXCIsXCJzdHJpbmdcIixcIm51bWJlclwiLFwiYm9vbGVhblwiXX1dfSxcImltcG9ydChcXFwiL1VzZXJzL29saXZpZXJzaW1hcmQtbW9yaXNzZXR0ZS9Eb2N1bWVudHMvR2l0aHViUHJvamVjdHMvZHVyYWJsZS1hY3RvcnMvc2RrL2Rpc3QvanNvblxcXCIse3dpdGg6e1xcXCJyZXNvbHV0aW9uLW1vZGVcXFwiOlxcXCJpbXBvcnRcXFwifX0pLkpzb25PYmplY3RcIjp7XCJ0eXBlXCI6XCJvYmplY3RcIixcImFkZGl0aW9uYWxQcm9wZXJ0aWVzXCI6e1wiJHJlZlwiOlwiIy9kZWZpbml0aW9ucy9PdXRnb2luZ1wifSxcInRpdGxlXCI6XCJKc29uT2JqZWN0XCJ9LFwiU3RhdGVcIjp7XCJ0eXBlXCI6XCJvYmplY3RcIixcInByb3BlcnRpZXNcIjp7fSxcInJlcXVpcmVkXCI6W119fX0sXCJlbWl0dGFibGVcIjpbXX19XTtcbiAgICAgICAgICAgICAgICBleHBvcnQgY29uc3QgdmVyc2lvbiA9IDE7Il0sCiAgIm1hcHBpbmdzIjogIjs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBQUE7QUFBQTtBQUFBO0FBQUE7QUFBQSxTQUFTLE9BQU8saUJBQWlCO0FBQWpDO0FBRUEsSUFBTSxrQkFBTixNQUFNLHlCQUF1QixZQUN6QixjQUFDLFlBRHdCLElBQU07QUFBQSxFQUFuQztBQUFBO0FBQ2Usd0JBQVEsU0FBUSxrQkFBaEIsZ0JBQWdCLEtBQWhCO0FBQUE7QUFBQSxFQUVYLE1BQU0sVUFBVSxT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
