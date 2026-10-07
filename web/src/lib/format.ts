import { toast } from "sonner"

export function formatTime(timestamp?: number | null, locale?: string) {
  return timestamp ? new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "medium" }).format(new Date(timestamp * 1000)) : "—"
}

export function formatTimeAgo(timestamp?: number | null, locale?: string) {
  if (!timestamp) return "—"
  const relativeSeconds = timestamp - Date.now() / 1000
  const [unit, seconds] = ([
    ["year", 31_557_600],
    ["month", 2_629_800],
    ["week", 604_800],
    ["day", 86_400],
    ["hour", 3600],
    ["minute", 60],
    ["second", 1],
  ] as const).find(([, duration]) => Math.abs(relativeSeconds) >= duration) ?? ["second", 1]
  return new Intl.RelativeTimeFormat(locale, { numeric: "auto" }).format(Math.round(relativeSeconds / seconds), unit)
}

export function formatBytes(bytes: number) {
  const units = ["B", "KB", "MB", "GB", "TB"]
  let value = bytes
  let unit = 0
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit += 1
  }
  return `${value >= 10 || unit === 0 ? value.toFixed(0) : value.toFixed(1)} ${units[unit]}`
}

export function showError(error: Error) {
  toast.error(error.message)
}
