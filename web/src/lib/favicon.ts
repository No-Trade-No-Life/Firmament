const FIRMAMENT_MARK_PATH = "M4 20a12 12 0 0 1 24 0M4 20a12 4 0 1 0 24 0 12 4 0 1 0-24 0M16 8v16"

const FAVICON_COLOR = {
  light: "#000",
  dark: "#fff",
} as const

// Browsers render the SVG favicon once and never re-evaluate its
// prefers-color-scheme styles, so a theme toggle leaves the old color in the
// tab until the next page load. Replacing the link with a data URL carrying
// the theme-colored mark updates the icon immediately.
export function applyFavicon(resolvedTheme: keyof typeof FAVICON_COLOR) {
  const color = FAVICON_COLOR[resolvedTheme]
  const mark = `<path d="${FIRMAMENT_MARK_PATH}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>`
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">${mark}</svg>`
  const link = document.querySelector<HTMLLinkElement>('link[rel="icon"]')!
  link.href = `data:image/svg+xml,${encodeURIComponent(svg)}`
}
