import { useEffect, useState } from "react"

export type ThemeChoice = "light" | "dark" | "system"

export const themeStorageKey = "firmament.theme"
const darkModeQuery = "(prefers-color-scheme: dark)"

let currentChoice: ThemeChoice | null = null
const listeners = new Set<() => void>()

function prefersDark() {
  return window.matchMedia(darkModeQuery).matches
}

function resolveDark(choice: ThemeChoice) {
  return choice === "dark" || (choice === "system" && prefersDark())
}

function readChoice(): ThemeChoice {
  try {
    const stored = window.localStorage.getItem(themeStorageKey)
    if (stored === "light" || stored === "dark" || stored === "system") {
      return stored
    }
  } catch (error) {
    console.warn("Firmament theme preference is unavailable; using the system scheme.", error)
  }
  return "system"
}

export function currentThemeChoice(): ThemeChoice {
  currentChoice ??= readChoice()
  return currentChoice
}

function applyChoice(choice: ThemeChoice) {
  const dark = resolveDark(choice)
  const root = document.documentElement
  root.classList.toggle("dark", dark)
  root.style.colorScheme = dark ? "dark" : "light"
}

export function setThemeChoice(choice: ThemeChoice) {
  currentChoice = choice
  try {
    window.localStorage.setItem(themeStorageKey, choice)
  } catch (error) {
    console.warn("Firmament could not persist the theme preference for this session.", error)
  }
  applyChoice(choice)
  listeners.forEach((listener) => listener())
}

function subscribe(listener: () => void) {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

export function useTheme() {
  const [choice, setChoice] = useState(currentThemeChoice)
  useEffect(() => {
    applyChoice(currentThemeChoice())
    return subscribe(() => setChoice(currentThemeChoice()))
  }, [])
  return { choice, setChoice: setThemeChoice }
}
