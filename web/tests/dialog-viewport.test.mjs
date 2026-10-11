import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import test from "node:test"

const modals = [
  { name: "dialog", file: "dialog.tsx", part: "Dialog" },
  { name: "alert dialog", file: "alert-dialog.tsx", part: "AlertDialog" },
]

test("keeps tall modal popups scrollable inside a viewport", async () => {
  for (const { name, file, part } of modals) {
    const source = await readFile(
      new URL(`../@/components/ui/${file}`, import.meta.url),
      "utf8"
    )

    const viewportStart = source.indexOf(`function ${part}Viewport`)
    assert.ok(viewportStart >= 0, `${name} renders a viewport`)
    const viewport = source.slice(
      viewportStart,
      source.indexOf(`function ${part}Content`)
    )
    assert.match(viewport, /fixed inset-0/)
    assert.match(viewport, /overflow-y-auto/)
    assert.match(viewport, /overscroll-contain/)

    const content = source.slice(
      source.indexOf(`function ${part}Content`),
      source.indexOf(`function ${part}Header`)
    )
    assert.match(content, new RegExp(`<${part}Viewport>`), `${name} popup is placed inside the viewport`)
    assert.match(content, /min-h-full/)
    assert.match(content, /items-center justify-center/)
    assert.doesNotMatch(content, /-translate-y-1\/2/, `${name} no longer centers by transform`)
    assert.doesNotMatch(content, /fixed top-1\/2/, `${name} no longer pins the popup off-screen`)
  }
})
