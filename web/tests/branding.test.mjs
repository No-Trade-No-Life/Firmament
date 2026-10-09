import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"
import { createRequire } from "node:module"
import { runInNewContext } from "node:vm"
import { createElement } from "react"
import { renderToStaticMarkup } from "react-dom/server"
import ts from "typescript"

import { applyFavicon } from "../src/lib/favicon.ts"

// Approved round dome from No-Trade-No-Life/home-page PR #6.
const DOME_PATH = "M4 20a12 12 0 0 1 24 0M4 20a12 4 0 1 0 24 0 12 4 0 1 0-24 0M16 8v16"

function assertDome(svg) {
  assert.equal(svg.match(/\bd="([^"]+)"/)?.[1], DOME_PATH)
  assert.match(svg, /viewBox="0 0 32 32"/)
  assert.match(svg, /fill="none"/)
  assert.match(svg, /stroke-width="1\.8"/)
  assert.match(svg, /stroke-linecap="round"/)
  assert.match(svg, /stroke-linejoin="round"/)
  assert.equal([...svg.matchAll(/<path\b/g)].length, 1)
  assert.doesNotMatch(svg, /<circle\b/)
}

test("application mark uses the approved dome and inherits its foreground", () => {
  const source = readFileSync(new URL("../src/components/firmament-mark.tsx", import.meta.url), "utf8")
  const compiled = ts.transpileModule(source, {
    compilerOptions: { jsx: ts.JsxEmit.ReactJSX, module: ts.ModuleKind.CommonJS },
  })
  const component = {}
  runInNewContext(compiled.outputText, { exports: component, require: createRequire(import.meta.url) })
  const svg = renderToStaticMarkup(createElement(component.FirmamentMark, { className: "size-7 shrink-0" }))
  assertDome(svg)
  assert.match(svg, /stroke="currentColor"/)
  assert.match(svg, /aria-hidden="true"/)
  assert.match(svg, /class="size-7 shrink-0"/)
})

test("static favicon uses the same dome with light and dark system colors", () => {
  const svg = readFileSync(new URL("../public/firmament-mark.svg", import.meta.url), "utf8")
  assertDome(svg)
  assert.match(svg, /aria-label="Firmament"/)
  assert.match(svg, /stroke: #000/)
  assert.match(svg, /@media \(prefers-color-scheme: dark\)/)
  assert.match(svg, /stroke: #fff/)
})

test("initial document versions the favicon URL to avoid the previous CDN cache", () => {
  const html = readFileSync(new URL("../index.html", import.meta.url), "utf8")
  assert.match(html, /rel="icon"[^>]*href="\/firmament-mark\.svg\?v=dome-v1"/)
})

test("theme changes regenerate the dome favicon in both directions", (t) => {
  const originalDocument = globalThis.document
  const link = { href: "/firmament-mark.svg" }
  globalThis.document = {
    querySelector(selector) {
      assert.equal(selector, 'link[rel="icon"]')
      return link
    },
  }
  t.after(() => { globalThis.document = originalDocument })

  for (const [theme, color] of [["light", "#000"], ["dark", "#fff"], ["light", "#000"]]) {
    const previous = link.href
    applyFavicon(theme)
    assert.notEqual(link.href, previous)
    assert.ok(link.href.startsWith("data:image/svg+xml,"))
    const svg = decodeURIComponent(link.href.slice("data:image/svg+xml,".length))
    assertDome(svg)
    assert.ok(svg.includes(`stroke="${color}"`))
  }
})
