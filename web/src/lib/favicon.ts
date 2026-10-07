const FIRMAMENT_MARK_PATHS = ["M4 25H28", "M6 25a10 10 0 0 1 20 0"]
const FIRMAMENT_MARK_DOTS = [
  { cx: 16, cy: 9.5, r: 1.7 },
  { cx: 10.5, cy: 14.5, r: 1.2 },
  { cx: 21.5, cy: 14.5, r: 1.2 },
  { cx: 14, cy: 19.5, r: 1 },
  { cx: 18, cy: 19.5, r: 1 },
] as const

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
  const paths = FIRMAMENT_MARK_PATHS.map((d) => `<path d="${d}" fill="none" stroke="${color}" stroke-width="2.2" stroke-linecap="round"/>`).join("")
  const dots = FIRMAMENT_MARK_DOTS.map(({ cx, cy, r }) => `<circle cx="${cx}" cy="${cy}" r="${r}" fill="${color}"/>`).join("")
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">${paths}${dots}</svg>`
  const link = document.querySelector<HTMLLinkElement>('link[rel="icon"]')!
  link.href = `data:image/svg+xml,${encodeURIComponent(svg)}`
}
