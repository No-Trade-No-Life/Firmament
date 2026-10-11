import { useEffect, useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { ArchiveIcon, DownloadIcon, FolderOpenIcon, PlusIcon } from "lucide-react"
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
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Textarea } from "@/components/ui/textarea"
import { downloadFile, request, type AuthSdk } from "../lib/api"
import { formatBytes, formatTime, showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { ArchiveJob, Dataset, DatasetManifest } from "../lib/types"

export function DatasetsPage({ auth, locale, t, isRoot }: { auth: AuthSdk; locale: string; t: Copy; isRoot: boolean }) {
  const datasets = useQuery({ queryKey: ["datasets"], queryFn: () => request<Dataset[]>("/api/v1/datasets", auth) })
  const [openId, setOpenId] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)
  const manifest = useQuery({
    queryKey: ["manifest", openId],
    queryFn: () => request<DatasetManifest>(`/api/v1/datasets/${openId}/manifest`, auth),
    enabled: openId !== null,
  })
  const list = datasets.data ?? []
  const openDataset = list.find((dataset) => dataset.id === openId) ?? null

  return <div className="flex flex-col gap-6">
    <div className="flex items-start justify-between gap-4">
      <div>
        <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.datasetsTitle}</h1>
        <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{t.datasetsDescription}</p>
      </div>
      {isRoot && <Button size="sm" onClick={() => setCreating(true)}><PlusIcon data-icon="inline-start" />{t.newDataset}</Button>}
    </div>
    {list.length === 0 ? <p className="text-sm text-muted-foreground">{t.noDatasets}</p> : <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
      {list.map((dataset) => <Card key={dataset.id}>
        <CardHeader>
          <div className="flex items-center justify-between gap-2"><CardTitle>{dataset.name}</CardTitle><Badge variant="outline">{dataset.tier === "hot" ? t.tierHot : t.tierCold}</Badge></div>
          <CardDescription>{dataset.description}</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <p className="text-xs text-muted-foreground">{t.files}: {dataset.file_count} · {formatBytes(dataset.bytes)}{dataset.cold_file_count > 0 ? ` · ${t.tierCold} ${dataset.cold_file_count}` : ""} · {t.updated} {formatTime(dataset.updated_at, locale)}</p>
          <div className="flex flex-wrap gap-2">
            <Button size="sm" variant="outline" onClick={() => setOpenId(dataset.id)}><FolderOpenIcon data-icon="inline-start" />{t.open}</Button>
            <Button size="sm" variant="ghost" onClick={() => void downloadFile(`/api/v1/datasets/${dataset.id}/manifest`, auth, `${dataset.id}-manifest.json`).catch(showError)}><DownloadIcon data-icon="inline-start" />{t.downloadManifest}</Button>
          </div>
          {isRoot && <ArchiveControls auth={auth} dataset={dataset} t={t} />}
        </CardContent>
      </Card>)}
    </div>}
    <Dialog open={openId !== null} onOpenChange={(open) => { if (!open) setOpenId(null) }}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>{openDataset?.name ?? t.datasets}</DialogTitle>
          <DialogDescription>{openDataset?.description ?? ""}</DialogDescription>
        </DialogHeader>
        {manifest.isPending && <p className="text-sm text-muted-foreground">…</p>}
        {manifest.data && manifest.data.files.length === 0 && <p className="text-sm text-muted-foreground">{t.emptyDataset}</p>}
        {manifest.data && manifest.data.files.length > 0 && <Table>
          <TableHeader><TableRow><TableHead>{t.files}</TableHead><TableHead>{t.statBytes}</TableHead><TableHead>{t.updated}</TableHead><TableHead className="text-right" /></TableRow></TableHeader>
          <TableBody>{manifest.data.files.map((file) => <TableRow key={file.path}>
            <TableCell className="max-w-72"><div className="flex items-center gap-2"><Badge variant="outline">{file.tier === "cold" ? t.tierCold : t.tierHot}</Badge><span className="truncate font-mono text-xs">{file.path}</span></div></TableCell>
            <TableCell className="text-muted-foreground">{formatBytes(file.size)}</TableCell>
            <TableCell className="text-muted-foreground">{formatTime(file.updated_at, locale)}</TableCell>
            <TableCell className="text-right"><Button size="sm" variant="ghost" onClick={() => { if (openId) void downloadFile(`/api/v1/datasets/${openId}/files/${encodeURI(file.path)}`, auth, file.path.split("/").pop() ?? file.path).catch(showError) }}><DownloadIcon /></Button></TableCell>
          </TableRow>)}</TableBody>
        </Table>}
      </DialogContent>
    </Dialog>
    <CreateDatasetDialog auth={auth} t={t} open={creating} onOpenChange={setCreating} />
  </div>
}

function CreateDatasetDialog({ auth, t, open, onOpenChange }: { auth: AuthSdk; t: Copy; open: boolean; onOpenChange: (open: boolean) => void }) {
  const queryClient = useQueryClient()
  const [id, setId] = useState("")
  const [name, setName] = useState("")
  const [description, setDescription] = useState("")
  const idOk = /^[a-z0-9][a-z0-9-]{0,63}$/.test(id)
  const create = useMutation({
    mutationFn: () => request<Dataset>("/api/v1/datasets", auth, { method: "POST", body: JSON.stringify({ id, name, description }) }),
    onSuccess: () => {
      toast.success(t.datasetCreated)
      void queryClient.invalidateQueries({ queryKey: ["datasets"] })
      setId("")
      setName("")
      setDescription("")
      onOpenChange(false)
    },
    onError: showError,
  })
  return <Dialog open={open} onOpenChange={onOpenChange}>
    <DialogContent>
      <DialogHeader><DialogTitle>{t.newDataset}</DialogTitle><DialogDescription>{t.datasetIdHint}</DialogDescription></DialogHeader>
      <form className="flex flex-col gap-4" onSubmit={(event) => { event.preventDefault(); create.mutate() }}>
        <FieldGroup>
          <Field><FieldLabel htmlFor="dataset-id">{t.datasetId}</FieldLabel><Input id="dataset-id" value={id} onChange={(event) => setId(event.target.value.toLowerCase())} placeholder={t.datasetIdPlaceholder} required /></Field>
          <Field><FieldLabel htmlFor="dataset-name">{t.datasetNameField}</FieldLabel><Input id="dataset-name" value={name} onChange={(event) => setName(event.target.value)} required /></Field>
          <Field><FieldLabel htmlFor="dataset-description">{t.datasetDescriptionField}</FieldLabel><Textarea id="dataset-description" value={description} onChange={(event) => setDescription(event.target.value)} rows={3} /></Field>
        </FieldGroup>
        <div className="flex justify-end gap-2">
          <Button type="button" variant="ghost" onClick={() => onOpenChange(false)}>{t.cancel}</Button>
          <Button type="submit" disabled={create.isPending || !idOk || name.trim() === ""}>{create.isPending ? t.creating : t.create}</Button>
        </div>
      </form>
    </DialogContent>
  </Dialog>
}

function ArchiveControls({ auth, dataset, t }: { auth: AuthSdk; dataset: Dataset; t: Copy }) {
  const queryClient = useQueryClient()
  const [confirming, setConfirming] = useState(false)
  const status = useQuery({
    queryKey: ["archive", dataset.id],
    queryFn: () => request<ArchiveJob | null>(`/api/v1/datasets/${dataset.id}/archive`, auth),
    refetchInterval: (query) => (query.state.data?.status === "running" ? 2000 : false),
  })
  const job = status.data ?? null
  const jobStatus = job?.status
  useEffect(() => {
    if (jobStatus && jobStatus !== "running") {
      void queryClient.invalidateQueries({ queryKey: ["datasets"] })
      void queryClient.invalidateQueries({ queryKey: ["manifest", dataset.id] })
    }
  }, [jobStatus, dataset.id, queryClient])
  const start = useMutation({
    mutationFn: () => request<ArchiveJob>(`/api/v1/datasets/${dataset.id}/archive`, auth, { method: "POST" }),
    onSuccess: () => { setConfirming(false); toast.success(t.archiveStarted); void queryClient.invalidateQueries({ queryKey: ["archive", dataset.id] }) },
    onError: (error) => { setConfirming(false); showError(error as Error) },
  })
  const processed = job ? job.archived_files + job.skipped_files + job.failed_files : 0
  const label = !job
    ? null
    : job.status === "running"
      ? `${t.archiveRunning} ${processed}/${job.total_files}`
      : `${t.archiveLatest}: ${t.archivedLabel} ${job.archived_files} · ${t.skippedLabel} ${job.skipped_files} · ${t.failedLabel} ${job.failed_files}${job.message ? ` · ${job.message}` : ""}`
  return <div className="flex items-center justify-between gap-2">
    <span className={`min-w-0 flex-1 truncate text-xs ${job?.status === "failed" ? "text-destructive" : "text-muted-foreground"}`}>{label ?? t.archiveHint}</span>
    <Button size="sm" variant="outline" disabled={job?.status === "running" || start.isPending} onClick={() => setConfirming(true)}>
      <ArchiveIcon data-icon="inline-start" />{t.archive}
    </Button>
    <AlertDialog open={confirming} onOpenChange={setConfirming}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{t.archiveConfirmTitle}</AlertDialogTitle>
          <AlertDialogDescription>{t.archiveConfirmDescription}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>{t.cancel}</AlertDialogCancel>
          <AlertDialogAction onClick={() => start.mutate()}>{t.archive}</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  </div>
}
