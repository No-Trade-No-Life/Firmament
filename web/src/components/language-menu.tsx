import { LanguagesIcon } from "lucide-react"

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
import { persistLocale, type Locale } from "../lib/i18n"

export function LanguageMenu({ locale, setLocale, label }: { locale: Locale; setLocale: (locale: Locale) => void; label: string }) {
  return <DropdownMenu><DropdownMenuTrigger render={<Button variant="ghost" size="icon-sm" aria-label={label} />}><LanguagesIcon /></DropdownMenuTrigger><DropdownMenuContent align="end"><DropdownMenuGroup><DropdownMenuLabel>{label}</DropdownMenuLabel><DropdownMenuRadioGroup value={locale} onValueChange={(value) => { const next = value as Locale; setLocale(next); persistLocale(next) }}><DropdownMenuRadioItem value="zh">中文</DropdownMenuRadioItem><DropdownMenuRadioItem value="en">English</DropdownMenuRadioItem></DropdownMenuRadioGroup></DropdownMenuGroup></DropdownMenuContent></DropdownMenu>
}
