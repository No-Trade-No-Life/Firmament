import { useEffect, useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { request, type AuthSdk } from "../lib/api"
import { formatBytes, formatTime, showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { ColdSettings, SystemResources } from "../lib/types"

function Meter({ value, total }: { value: number; total: number }) {
  const ratio = total <= 0 ? 0 : Math.min(100, (value / total) * 100)
  return <div className="h-2 w-full overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full bg-primary" style={{ width: `${ratio}%` }} /></div>
}

function Row({ label, value }: { label: string; value: string }) {
  return <div className="flex items-center justify-between gap-3"><span className="text-muted-foreground">{label}</span><span className="font-mono text-xs">{value}</span></div>
}

export function SystemResourcesPage({ auth, locale, t }: { auth: AuthSdk; locale: string; t: Copy }) {
  const resources = useQuery({
    queryKey: ["system-resources"],
    queryFn: () => request<SystemResources>("/api/v1/system/resources", auth),
    refetchInterval: 5000,
  })
  if (resources.isPending || !resources.data) return <p className="text-sm text-muted-foreground">{t.signin}</p>
  const data = resources.data
  return <div className="flex flex-col gap-6">
    <div><h1 className="m-0 text-2xl font-semibold tracking-tight">{t.resourcesTitle}</h1><p className="mt-1 text-sm text-muted-foreground">{t.resourcesDescription}</p></div>
    <div className="grid gap-4 md:grid-cols-2">
      <Card><CardHeader><CardTitle>{t.cpu}</CardTitle><CardDescription>{t.cpuUsage} · {t.load1m} · {t.cpuCores}</CardDescription></CardHeader><CardContent className="flex flex-col gap-3"><Meter value={data.cpu.usage_percent} total={100} /><Row label={t.cpuUsage} value={`${data.cpu.usage_percent.toFixed(1)}%`} /><Row label={t.cpuCores} value={String(data.cpu.logical_cpus)} /><Row label={t.load1m} value={data.cpu.load_1m.toFixed(2)} /></CardContent></Card>
      <Card><CardHeader><CardTitle>{t.memory}</CardTitle><CardDescription>{t.memoryUsed} / {formatBytes(data.memory.total_bytes)}</CardDescription></CardHeader><CardContent className="flex flex-col gap-3"><Meter value={data.memory.used_bytes} total={data.memory.total_bytes} /><Row label={t.memoryUsed} value={formatBytes(data.memory.used_bytes)} /><Row label={t.memoryAvailable} value={formatBytes(data.memory.available_bytes)} /></CardContent></Card>
      {data.disk && <Card><CardHeader><CardTitle>{t.disk}</CardTitle><CardDescription>{t.mountPoint}: {data.disk.mount_point}</CardDescription></CardHeader><CardContent className="flex flex-col gap-3"><Meter value={data.disk.used_bytes} total={data.disk.total_bytes} /><Row label={t.diskUsed} value={formatBytes(data.disk.used_bytes)} /><Row label={t.diskAvailable} value={formatBytes(data.disk.available_bytes)} /></CardContent></Card>}
      <Card><CardHeader><CardTitle>{t.sqlite}</CardTitle><CardDescription>{t.sqliteTotal}: {formatBytes(data.sqlite.total_bytes)}</CardDescription></CardHeader><CardContent className="flex flex-col gap-2"><Row label={t.sqliteMain} value={formatBytes(data.sqlite.main_bytes)} /><Row label={t.sqliteWal} value={formatBytes(data.sqlite.wal_bytes)} /><Row label={t.sqliteShm} value={formatBytes(data.sqlite.shm_bytes)} /></CardContent></Card>
      <ColdStorageCard auth={auth} t={t} />
    </div>
    <p className="text-xs text-muted-foreground">{t.updated} {formatTime(data.sampled_at, locale)}</p>
  </div>
}

function ColdStorageCard({ auth, t }: { auth: AuthSdk; t: Copy }) {
  const queryClient = useQueryClient()
  const settings = useQuery({ queryKey: ["cold-settings"], queryFn: () => request<ColdSettings>("/api/v1/cold", auth) })
  const [bucket, setBucket] = useState("")
  const [region, setRegion] = useState("")
  useEffect(() => {
    if (settings.data) {
      setBucket(settings.data.bucket)
      setRegion(settings.data.region)
    }
  }, [settings.data])
  const save = useMutation({
    mutationFn: () => request<ColdSettings>("/api/v1/cold", auth, { method: "PUT", body: JSON.stringify({ bucket, region }) }),
    onSuccess: () => { toast.success(t.coldSaved); void queryClient.invalidateQueries({ queryKey: ["cold-settings"] }) },
    onError: showError,
  })
  if (settings.isPending || !settings.data) return null
  return <Card>
    <CardHeader><CardTitle>{t.coldStorage}</CardTitle><CardDescription>{t.coldStorageDescription}</CardDescription></CardHeader>
    <CardContent className="flex flex-col gap-4">
      <FieldGroup>
        <div className="grid gap-4 sm:grid-cols-2">
          <Field><FieldLabel htmlFor="cold-bucket">{t.coldBucket}</FieldLabel><Input id="cold-bucket" value={bucket} onChange={(event) => setBucket(event.target.value)} /></Field>
          <Field><FieldLabel htmlFor="cold-region">{t.coldRegion}</FieldLabel><Input id="cold-region" value={region} onChange={(event) => setRegion(event.target.value)} /></Field>
        </div>
      </FieldGroup>
      <div><Button size="sm" disabled={save.isPending || bucket === "" || region === ""} onClick={() => save.mutate()}>{save.isPending ? t.saving : t.save}</Button></div>
    </CardContent>
  </Card>
}
