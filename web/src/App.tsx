import { useEffect, useState } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { useAuthMini } from "auth-mini-react-components"
import { LinkitProvider, useLinkit } from "linkit-react-components"
import { Navigate, Route, Routes, useLocation } from "react-router-dom"
import { BotIcon, DatabaseIcon, HardDriveDownloadIcon, HardDriveIcon, LayoutDashboardIcon, RefreshCwIcon } from "lucide-react"
import { AppLayout, type AppNavGroup } from "@zccz14/ux"

import { FirmamentMark } from "./components/firmament-mark"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { Toaster } from "@/components/ui/sonner"
import { TooltipProvider } from "@/components/ui/tooltip"
import { request, type AuthSdk } from "./lib/api"
import { applyFavicon } from "./lib/favicon"
import { copy, initialLocale, negotiateLocale, persistLocale, type Copy, type Locale } from "./lib/i18n"
import type { LinkitStatus, Me } from "./lib/types"
import { DatasetsPage } from "./pages/datasets-page"
import { LinkitPage } from "./pages/linkit-page"
import { OverviewPage } from "./pages/overview-page"
import { SetupPage } from "./pages/setup-page"
import { SyncPage } from "./pages/sync-page"
import { SystemResourcesPage } from "./pages/system-resources-page"

export default function App() {
  const { isReady, isAuthenticated, sdk } = useAuthMini()
  const [locale, setLocale] = useState<Locale>(initialLocale)
  useEffect(() => {
    persistLocale(locale)
    document.documentElement.lang = locale === "zh" ? "zh-CN" : "en"
  }, [locale])
  const t = copy[locale]
  if (!isReady || !isAuthenticated || !sdk) return <div className="grid min-h-svh place-items-center text-sm text-muted-foreground">{t.signin}</div>
  return <LinkitProvider linkitBaseUrl="https://linkit.ntnl.io" lang={locale}><LinkitLanguageSync setLocale={setLocale} /><FaviconSync /><LinkitAutoEnsure auth={sdk} /><FirmamentShell auth={sdk} locale={locale} t={t} /></LinkitProvider>
}

// The signed-in Linkit profile owns the language preference; follow it
// instead of rendering a separate switcher in the application header.
function LinkitLanguageSync({ setLocale }: { setLocale: (locale: Locale) => void }) {
  const { languages } = useLinkit()
  useEffect(() => {
    const next = negotiateLocale(languages)
    if (next) setLocale(next)
  }, [languages, setLocale])
  return null
}

// Keep the favicon in step with the resolved theme without a reload.
function FaviconSync() {
  const { resolvedTheme } = useLinkit()
  useEffect(() => {
    applyFavicon(resolvedTheme)
  }, [resolvedTheme])
  return null
}

// Firmament keeps one Linkit Bot per user; this silent call provisions or
// repairs the connection on every workspace load, so notifications never need
// a setup step.
function LinkitAutoEnsure({ auth }: { auth: AuthSdk }) {
  const client = useQueryClient()
  const ensure = useQuery({
    queryKey: ["linkit-ensure", auth.session.getState().sessionId],
    queryFn: () => request<LinkitStatus>("/api/v1/linkit", auth, { method: "POST" }),
    retry: false,
    staleTime: Infinity,
  })
  useEffect(() => {
    if (!ensure.isSuccess) return
    void client.invalidateQueries({ queryKey: ["linkit"] })
  }, [client, ensure.isSuccess])
  return null
}

function FirmamentShell({ auth, locale, t }: { auth: AuthSdk; locale: Locale; t: Copy }) {
  const { resolvedTheme } = useLinkit()
  const queryClient = useQueryClient()
  const location = useLocation()
  const me = useQuery({ queryKey: ["me"], queryFn: () => request<Me>("/api/v1/me", auth) })
  const refresh = () => void queryClient.invalidateQueries()
  if (me.isPending || !me.data) return <div className="grid min-h-svh place-items-center"><Skeleton className="h-8 w-48" /></div>
  if (me.error) return <div className="grid min-h-svh place-items-center text-sm text-muted-foreground">{me.error.message}</div>
  if (me.data.setup_required && location.pathname !== "/setup") return <Navigate to="/setup" replace />
  if (!me.data.setup_required && location.pathname === "/setup") return <Navigate to="/" replace />

  const nav: AppNavGroup[] = [
    {
      label: t.navWorkspace,
      items: [
        { to: "/", label: t.overview, icon: <LayoutDashboardIcon /> },
        { to: "/datasets", label: t.datasets, icon: <DatabaseIcon /> },
        { to: "/sync", label: t.sync, icon: <HardDriveDownloadIcon /> },
        { to: "/linkit", label: t.linkit, icon: <BotIcon /> },
      ],
    },
    ...(me.data.is_root ? [{ label: t.navSystem, items: [{ to: "/system", label: t.systemResources, icon: <HardDriveIcon /> }] }] : []),
  ]

  return (
    <TooltipProvider>
      <Toaster position="top-center" theme={resolvedTheme} />
      <AppLayout
        logo={{ light: <FirmamentMark className="size-7 shrink-0" />, dark: <FirmamentMark className="size-7 shrink-0" /> }}
        title={t.appName}
        nav={nav}
        pageTitle={pageTitle(location.pathname, t)}
        headerSlot={<div className="flex items-center gap-1">{me.data.is_root && <Badge variant="outline">{t.root}</Badge>}<Button variant="ghost" size="icon-sm" onClick={refresh} aria-label={t.refresh}><RefreshCwIcon /></Button></div>}
      >
        <Routes>
          <Route path="/" element={<OverviewPage auth={auth} locale={locale} t={t} />} />
          <Route path="/datasets" element={<DatasetsPage auth={auth} locale={locale} t={t} isRoot={me.data.is_root} />} />
          <Route path="/sync" element={<SyncPage auth={auth} locale={locale} t={t} />} />
          <Route path="/linkit" element={<LinkitPage auth={auth} locale={locale} t={t} />} />
          <Route path="/system" element={me.data.is_root ? <SystemResourcesPage auth={auth} locale={locale} t={t} /> : <Navigate to="/" replace />} />
          <Route path="/setup" element={<SetupPage auth={auth} t={t} onDone={refresh} />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AppLayout>
    </TooltipProvider>
  )
}

function pageTitle(pathname: string, t: Copy) {
  if (pathname.startsWith("/datasets")) return t.datasets
  if (pathname.startsWith("/sync")) return t.sync
  if (pathname === "/linkit") return t.linkit
  if (pathname === "/system") return t.systemResources
  return t.overview
}
