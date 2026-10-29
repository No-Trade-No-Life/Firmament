import { useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { PlusIcon, RefreshCcwIcon, Trash2Icon } from "lucide-react"
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
import { Field, FieldDescription, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { errorMessage, request, requestBytes, type AuthSdk } from "../lib/api"
import { formatTime, showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { DatasetView, Manifest, Syncer } from "../lib/types"
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
  const datasets = useQuery({ queryKey: ["datasets"], queryFn: () => request<DatasetView[]>("/api/v1/datasets", auth) })
  const [name, setName] = useState("")
  const [datasetId, setDatasetId] = useState("")
  const [progress, setProgress] = useState<Record<string, SyncProgress>>({})
  const [busy, setBusy] = useState<string | null>(null)
  const [removing, setRemoving] = useState<Syncer | null>(null)
  const supported = supportsDirectorySync()
  const list = syncers.data ?? []

  const create = useMutation({
    mutationFn: () => request<Syncer>("/api/v1/syncers", auth, {
      method: "POST",
      body: JSON.stringify({ name, dataset_id: datasetId }),
    }),
    onSuccess: () => {
      setName("")
      void queryClient.invalidateQueries({ queryKey: ["syncers"] })
    },
    onError: (error) => showError(error, t),
  })
  const remove = useMutation({
    mutationFn: (syncer: Syncer) => request<void>(`/api/v1/syncers/${syncer.id}`, auth, { method: "DELETE" }),
    onSuccess: () => {
      setRemoving(null)
      void queryClient.invalidateQueries({ queryKey: ["syncers"] })
    },
    onError: (error) => showError(error, t),
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
      // Incremental by revision: the manifest only carries what this mirror has not
      // seen yet, so a repeated sync downloads nothing.
      const query = new URLSearchParams({ since: String(syncer.last_revision) })
      const manifest = await request<Manifest>(`/api/v1/datasets/${syncer.dataset_id}/manifest?${query.toString()}`, auth)
      const summary = await runSync({
        directory: handle,
        datasetId: syncer.dataset_id,
        latestRevision: manifest.latest_revision,
        files: manifest.files.map((file) => ({ path: file.path, size: file.bytes, sha256: file.sha256 })),
        download: (path) => requestBytes(`/api/v1/datasets/${syncer.dataset_id}/files/${encodeURI(path)}`, auth),
        onProgress: (value) => setProgress((previous) => ({ ...previous, [syncer.id]: value })),
      })
      await request(`/api/v1/syncers/${syncer.id}/completed`, auth, { method: "POST", body: JSON.stringify({ revision: summary.latestRevision }) })
      toast.success(`${t.syncCompleted}: ↓${summary.downloaded} · ✕${summary.removed} · =${summary.skipped}`)
      void queryClient.invalidateQueries({ queryKey: ["syncers"] })
    } catch (error) {
      const message = errorMessage(error, t)
      setProgress((previous) => ({ ...previous, [syncer.id]: { phase: "failed", total: 0, completed: 0, current: "", error: message } }))
      showError(error, t)
    } finally {
      setBusy(null)
    }
  }

  return <div className="flex flex-col gap-6">
    <div>
      <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.syncTitle}</h1>
      <p className="mt-1 max-w-3xl text-sm text-muted-foreground">{t.syncDescription}</p>
    </div>
    {!supported && <p className="text-sm text-muted-foreground">{t.syncUnsupported}</p>}

    <Card>
      <CardHeader><CardTitle>{t.newSyncer}</CardTitle></CardHeader>
      <CardContent>
        <form className="flex flex-col gap-4" onSubmit={(event) => { event.preventDefault(); create.mutate() }}>
          <FieldGroup>
            <div className="grid gap-4 sm:grid-cols-2">
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
            </div>
          </FieldGroup>
          <FieldDescription>{t.syncIncrementalHint}</FieldDescription>
          <div><Button type="submit" disabled={create.isPending || !datasetId}><PlusIcon data-icon="inline-start" />{create.isPending ? t.creating : t.create}</Button></div>
        </form>
      </CardContent>
    </Card>

    <Card>
      <CardHeader><CardTitle>{t.syncTitle}</CardTitle></CardHeader>
      <CardContent>
        {list.length === 0 ? <p className="text-sm text-muted-foreground">{t.noSyncers}</p> : <Table>
          <TableHeader><TableRow>
            <TableHead>{t.syncerName}</TableHead>
            <TableHead>{t.syncerDataset}</TableHead>
            <TableHead>{t.mirroredRevision}</TableHead>
            <TableHead>{t.lastSynced}</TableHead>
            <TableHead className="text-right" />
          </TableRow></TableHeader>
          <TableBody>{list.map((syncer) => {
            const state = progress[syncer.id]
            return <TableRow key={syncer.id}>
              <TableCell className="font-medium">{syncer.name}</TableCell>
              <TableCell className="font-mono text-xs">{syncer.dataset_id}</TableCell>
              <TableCell className="font-mono text-xs">{syncer.last_revision}</TableCell>
              <TableCell className="text-muted-foreground">
                {syncer.last_synced_at ? formatTime(syncer.last_synced_at, locale) : t.neverSynced}
                {state && state.phase !== "completed" && state.phase !== "failed" && <span className="ml-2 text-xs">{phaseLabel(state, t)}</span>}
                {state?.phase === "failed" && <span className="ml-2 text-xs text-destructive">{state.error}</span>}
              </TableCell>
              <TableCell className="text-right">
                <div className="flex justify-end gap-1">
                  <Button size="sm" variant="outline" disabled={!supported || busy === syncer.id} onClick={() => void run(syncer, false)}>
                    <RefreshCcwIcon data-icon="inline-start" />
                    {syncer.last_synced_at ? t.syncResume : t.syncPick}
                  </Button>
                  <Button size="icon-sm" variant="ghost" aria-label={t.remove} onClick={() => setRemoving(syncer)}><Trash2Icon /></Button>
                </div>
              </TableCell>
            </TableRow>
          })}</TableBody>
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
          <AlertDialogAction onClick={() => { if (removing) remove.mutate(removing) }}>{t.remove}</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  </div>
}

function phaseLabel(state: SyncProgress, t: Copy) {
  if (state.phase === "scanning") return t.progressScanning
  if (state.phase === "downloading") return `${t.progressDownloading} ${state.completed}/${state.total}`
  if (state.phase === "cleaning") return t.progressCleaning
  if (state.phase === "failed") return t.progressFailed
  return t.progressCompleted
}
