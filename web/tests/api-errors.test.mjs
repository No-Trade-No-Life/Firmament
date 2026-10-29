import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"
import { createRequire } from "node:module"
import { runInNewContext } from "node:vm"
import ts from "typescript"

const source = readFileSync(new URL("../src/lib/api.ts", import.meta.url), "utf8")

// The module is loaded the same way the other tests inspect components: compile
// the TypeScript source and run it with a stubbed transport, so the failure
// wording is checked without a browser or a live server.
function load(fetch) {
  const compiled = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  })
  const api = {}
  runInNewContext(compiled.outputText, {
    exports: api,
    module: { exports: api },
    require: createRequire(import.meta.url),
    fetch,
    Response,
    Headers,
    URL,
    setTimeout,
    console,
  })
  return api
}

const copy = {
  error: "出错了",
  errorNetwork: "无法连接到服务，请检查网络后重试",
  errorRejected: "服务端拒绝了这次请求",
  errorServerUnavailable: "服务暂时不可用，请稍后重试",
  errorBadResponse: "服务返回了无法识别的响应",
}

const failure = (promise) => promise.then(() => null, (error) => error)

test("a text/plain rejection names the server reason instead of a JSON parse error", async () => {
  const api = load(async () => new Response(
    "Failed to deserialize the JSON body into the target type: missing field `revision` at line 1 column 27",
    { status: 422, headers: { "Content-Type": "text/plain" } },
  ))
  const error = await failure(api.request("/api/v1/syncers/x/completed"))
  assert.ok(error instanceof api.RequestError)
  assert.equal(error.kind, "status")
  assert.equal(error.status, 422)
  const message = api.errorMessage(error, copy)
  assert.match(message, /^服务端拒绝了这次请求 · HTTP 422 · /)
  assert.match(message, /missing field `revision`/)
  assert.doesNotMatch(message, /Unexpected token/)
})

test("the JSON error envelope is shown verbatim", async () => {
  const api = load(async () => new Response(
    JSON.stringify({ error: "invalid request body: missing field `revision`" }),
    { status: 422, headers: { "Content-Type": "application/json" } },
  ))
  const error = await failure(api.request("/x"))
  assert.equal(
    api.errorMessage(error, copy),
    "服务端拒绝了这次请求 · HTTP 422 · invalid request body: missing field `revision`",
  )
})

test("an HTML gateway failure reads as an outage instead of dumping markup", async () => {
  const api = load(async () => new Response("<html><body>502 Bad Gateway</body></html>", {
    status: 502,
    headers: { "Content-Type": "text/html" },
  }))
  const error = await failure(api.request("/api/v1/syncers"))
  assert.equal(api.errorMessage(error, copy), "服务暂时不可用，请稍后重试 · HTTP 502")
})

test("a success answer that is not JSON is reported as an unreadable response", async () => {
  const api = load(async () => new Response("maintenance", { status: 200 }))
  const error = await failure(api.request("/api/v1/syncers"))
  assert.equal(error.kind, "body")
  assert.equal(api.errorMessage(error, copy), "服务返回了无法识别的响应 · maintenance")
})

test("a transport failure reads as a connection problem", async () => {
  const api = load(async () => {
    throw new TypeError("fetch failed")
  })
  const error = await failure(api.request("/api/v1/syncers"))
  assert.equal(error.kind, "network")
  assert.equal(api.errorMessage(error, copy), "无法连接到服务，请检查网络后重试")
})

test("a JSON body still parses and an empty one stays undefined", async () => {
  const api = load(async () => new Response(JSON.stringify({ revision: 5 }), { status: 200 }))
  assert.equal((await api.request("/x")).revision, 5)

  const empty = load(async () => new Response(null, { status: 204 }))
  assert.equal(await empty.request("/x"), undefined)
})
