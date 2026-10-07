import { MonitorIcon, MoonIcon, SunIcon } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { useTheme, type ThemeChoice } from "@/lib/theme"

export function ThemeSwitcher({ label, light, dark, system }: { label: string; light: string; dark: string; system: string }) {
  const { choice, setChoice } = useTheme()
  const options: { value: ThemeChoice; label: string }[] = [
    { value: "light", label: light },
    { value: "dark", label: dark },
    { value: "system", label: system },
  ]
  const ActiveIcon = choice === "light" ? SunIcon : choice === "dark" ? MoonIcon : MonitorIcon

  return <DropdownMenu><DropdownMenuTrigger render={<Button variant="ghost" size="icon-sm" aria-label={label} />}><ActiveIcon /></DropdownMenuTrigger><DropdownMenuContent align="end"><DropdownMenuGroup><DropdownMenuLabel>{label}</DropdownMenuLabel><DropdownMenuRadioGroup value={choice} onValueChange={(value) => setChoice(value as ThemeChoice)}>{options.map((option) => <DropdownMenuRadioItem key={option.value} value={option.value}>{option.label}</DropdownMenuRadioItem>)}</DropdownMenuRadioGroup></DropdownMenuGroup></DropdownMenuContent></DropdownMenu>
}
