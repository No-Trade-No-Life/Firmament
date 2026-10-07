import { useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { FolderOpenIcon, PlusIcon, RefreshCcwIcon, Trash2Icon } from "lucide-react"
import { toast } from "sonner"

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { request, requestBytes, type AuthSdk } from "../lib/api"
import { formatTimeAgo, showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { Dataset, DatasetManifest, Syncer } from "../lib/types"
import {
  ensurePermission,
  loadDirectoryHandle,
  pickDirectory,
  runSync,
  saveDirectoryHandle,
  supportsDirectorySync,
  type DirectoryHandleLike,
  type SyncProgress,
} from "../lib/sync"

export function SyncPage({ auth, locale, t }: { auth: AuthSdk; locale: string; t: Copy }) {
  const queryClient = useQueryClient()
  const syncers = useQuery({ queryKey: ["syncers"], queryFn: () => request<Syncer[]>("/api/v1/syncers", auth) })
  const datasets = useQuery({ queryKey: ["datasets"], queryFn: () => request<Dataset[]>("/api/v1/datasets", auth) })
  const [name, setName] = useState("")
  const [datasetId, setDatasetId] = useState("")
  const [subdir, setSubdir] = useState("")
  const [progress, setProgress] = useState<Record<string, SyncProgress>>({})
  const [busy, setBusy] = useState<string | null>(null)
  const [removing, setRemoving] = useState<Syncer | null>(null)
  const supported = supportsDirectorySync()
  const list = syncers.data ?? []

  const create = useMutation({
    mutationFn: () => request<Syncer>("/api/v1/syncers", auth, { method: "POST", body: JSON.stringify({ name, dataset_id: datasetId, subdir }) }),
    onSuccess: () => { setName(""); setSubdir(""); void queryClient.invalidateQueries({ queryKey: ["syncers"] }) },
    onError: showError,
  })
  const remove = useMutation({
    mutationFn: (syncer: Syncer) => request<void>(`/api/v1/syncers/${syncer.id}`, auth, { method: "DELETE" }),
    onSuccess: () => { setRemoving(null); void queryClient.invalidateQueries({ queryKey: ["syncers"] }) },
    onError: showError,
  })

  async function run(syncer: Syncer, pick: boolean) {
    setBusy(syncer.id)
    setProgress((previous) => ({ ...previous, [syncer.id]: { phase: "scanning", total: 0, completed: 0, current: "" } }))
    try {
      let handle: DirectoryHandleLike | null = pick ? null : await loadDirectoryHandle(syncer.id)
      if (handle && !(await ensurePermission(handle))) handle = null
      if (!handle) {
        handle = await pickDirectory()
        if (!(await ensurePermission(handle))) throw new Error(t.syncPermissionDenied)
        await saveDirectoryHandle(syncer.id, handle)
      }
      const manifest = await request<DatasetManifest>(`/api/v1/datasets/${syncer.dataset_id}/manifest`, auth)
      const summary = await runSync({
        directory: handle,
        datasetId: syncer.dataset_id,
        prefix: syncer.subdir,
        files: manifest.files,
        download: (path) => requestBytes(`/api/v1/datasets/${syncer.dataset_id}/files/${encodeURI(path)}`, auth),
        onProgress: (value) => setProgress((previous) => ({ ...previous, [syncer.id]: value })),
      })
      await request(`/api/v1/syncers/${syncer.id}/completed`, auth, { method: "POST" })
      toast.success(`${t.syncCompleted}: ↓${summary.downloaded} · ✕${summary.removed} · =${summary.skipped}`)
      void queryClient.invalidateQueries({ queryKey: ["syncers"] })
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      setProgress((previous) => ({ ...previous, [syncer.id]: { phase: "failed", total: 0, completed: 0, current: "", error: message } }))
      showError(error as Error)
    } finally {
      setBusy(null)
    }
  }

  return <div className="flex flex-col gap-6">
    <div>
      <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.syncTitle}</h1>
      <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{t.syncDescription}</p>
    </div>
    {!supported && <p className="text-sm text-muted-foreground">{t.syncUnsupported}</p>}
    <Card>
      <CardHeader><CardTitle>{t.newSyncer}</CardTitle></CardHeader>
      <CardContent>
        <form className="flex flex-col gap-4" onSubmit={(event) => { event.preventDefault(); create.mutate() }}>
          <FieldGroup>
            <div className="grid gap-4 sm:grid-cols-3">
              <Field>
                <FieldLabel htmlFor="syncer-name">{t.syncerName}</FieldLabel>
                <Input id="syncer-name" value={name} onChange={(event) => setName(event.target.value)} required />
              </Field>
              <Field>
                <FieldLabel>{t.syncerDataset}</FieldLabel>
                <Select value={datasetId} onValueChange={(value) => setDatasetId(value ?? "")}>
                  <SelectTrigger><SelectValue placeholder={t.chooseDataset} /></SelectTrigger>
                  <SelectContent>{(datasets.data ?? []).map((dataset) => <SelectItem key={dataset.id} value={dataset.id}>{dataset.name}</SelectItem>)}</SelectContent>
                </Select>
              </Field>
              <Field>
                <FieldLabel htmlFor="syncer-subdir">{t.syncerSubdir}</FieldLabel>
                <Input id="syncer-subdir" value={subdir} onChange={(event) => setSubdir(event.target.value)} placeholder="btc/" />
              </Field>
            </div>
          </FieldGroup>
          <div><Button type="submit" disabled={create.isPending || datasetId === ""}><PlusIcon data-icon="inline-start" />{create.isPending ? t.creating : t.create}</Button></div>
        </form>
      </CardContent>
    </Card>
    <Card>
      <CardHeader><CardTitle>{t.syncTitle}</CardTitle></CardHeader>
      <CardContent>
        {list.length === 0 ? <Empty><EmptyHeader><EmptyMedia variant="icon"><RefreshCcwIcon /></EmptyMedia><EmptyTitle>{t.noSyncers}</EmptyTitle><EmptyDescription>{t.syncDescription}</EmptyDescription></EmptyHeader></Empty> : <Table>
          <TableHeader><TableRow><TableHead>{t.syncerName}</TableHead><TableHead>{t.syncerDataset}</TableHead><TableHead>{t.lastSynced}</TableHead><TableHead className="text-right" /></TableRow></TableHeader>
          <TableBody>{list.map((syncer) => <TableRow key={syncer.id}>
            <TableCell className="min-w-48 align-top">
              <span className="font-medium">{syncer.name}</span>
              {syncer.subdir !== "" && <span className="ml-2 font-mono text-xs text-muted-foreground">/{syncer.subdir}</span>}
              <SyncStatus state={progress[syncer.id]} t={t} />
            </TableCell>
            <TableCell className="align-top">{syncer.dataset_id}</TableCell>
            <TableCell className="align-top text-muted-foreground">{syncer.last_synced_at ? formatTimeAgo(syncer.last_synced_at, locale) : t.neverSynced}</TableCell>
            <TableCell className="text-right align-top"><div className="flex justify-end gap-2">
              <Button size="sm" variant="outline" disabled={!supported || busy === syncer.id} onClick={() => void run(syncer, true)}><FolderOpenIcon data-icon="inline-start" />{t.syncPick}</Button>
              <Button size="sm" variant="ghost" disabled={!supported || busy === syncer.id} onClick={() => void run(syncer, false)}><RefreshCcwIcon data-icon="inline-start" />{t.syncResume}</Button>
              <Button size="sm" variant="ghost" disabled={busy === syncer.id} onClick={() => setRemoving(syncer)} aria-label={t.remove}><Trash2Icon /></Button>
            </div></TableCell>
          </TableRow>)}</TableBody>
        </Table>}
      </CardContent>
    </Card>
    <AlertDialog open={removing !== null} onOpenChange={(open) => { if (!open) setRemoving(null) }}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{t.removeTitle}</AlertDialogTitle>
          <AlertDialogDescription>{t.removeDescription}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>{t.cancel}</AlertDialogCancel>
          <AlertDialogAction onClick={() => removing && remove.mutate(removing)}>{t.remove}</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  </div>
}

function SyncStatus({ state, t }: { state?: SyncProgress; t: Copy }) {
  if (!state) return null
  const label = state.phase === "scanning" ? t.progressScanning : state.phase === "downloading" ? t.progressDownloading : state.phase === "cleaning" ? t.progressCleaning : state.phase === "completed" ? t.progressCompleted : t.progressFailed
  const ratio = state.phase === "failed" ? 0 : state.total === 0 ? (state.phase === "completed" ? 100 : 0) : Math.min(100, (state.completed / state.total) * 100)
  return <div className="mt-2 flex max-w-72 flex-col gap-1">
    <span className="truncate text-xs text-muted-foreground">{state.error ?? `${label}${state.current ? ` · ${state.current}` : ""}`}</span>
    <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full bg-primary" style={{ width: `${ratio}%` }} /></div>
  </div>
}
