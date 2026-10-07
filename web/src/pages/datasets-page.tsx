import { useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { DownloadIcon, FolderOpenIcon } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { downloadFile, request, type AuthSdk } from "../lib/api"
import { formatBytes, formatTime, showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { Dataset, DatasetManifest } from "../lib/types"

export function DatasetsPage({ auth, locale, t }: { auth: AuthSdk; locale: string; t: Copy }) {
  const datasets = useQuery({ queryKey: ["datasets"], queryFn: () => request<Dataset[]>("/api/v1/datasets", auth) })
  const [openId, setOpenId] = useState<string | null>(null)
  const manifest = useQuery({
    queryKey: ["manifest", openId],
    queryFn: () => request<DatasetManifest>(`/api/v1/datasets/${openId}/manifest`, auth),
    enabled: openId !== null,
  })
  const list = datasets.data ?? []
  const openDataset = list.find((dataset) => dataset.id === openId) ?? null

  return <div className="flex flex-col gap-6">
    <div>
      <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.datasetsTitle}</h1>
      <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{t.datasetsDescription}</p>
    </div>
    {list.length === 0 ? <p className="text-sm text-muted-foreground">{t.noDatasets}</p> : <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
      {list.map((dataset) => <Card key={dataset.id}>
        <CardHeader>
          <div className="flex items-center justify-between gap-2"><CardTitle>{dataset.name}</CardTitle><Badge variant="outline">{dataset.tier === "hot" ? t.tierHot : t.tierCold}</Badge></div>
          <CardDescription>{dataset.description}</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <p className="text-xs text-muted-foreground">{t.files}: {dataset.file_count} · {formatBytes(dataset.bytes)} · {t.updated} {formatTime(dataset.updated_at, locale)}</p>
          <div className="flex flex-wrap gap-2">
            <Button size="sm" variant="outline" onClick={() => setOpenId(dataset.id)}><FolderOpenIcon data-icon="inline-start" />{t.open}</Button>
            <Button size="sm" variant="ghost" onClick={() => void downloadFile(`/api/v1/datasets/${dataset.id}/manifest`, auth, `${dataset.id}-manifest.json`).catch(showError)}><DownloadIcon data-icon="inline-start" />{t.downloadManifest}</Button>
          </div>
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
            <TableCell className="max-w-72 truncate font-mono text-xs">{file.path}</TableCell>
            <TableCell className="text-muted-foreground">{formatBytes(file.size)}</TableCell>
            <TableCell className="text-muted-foreground">{formatTime(file.updated_at, locale)}</TableCell>
            <TableCell className="text-right"><Button size="sm" variant="ghost" onClick={() => { if (openId) void downloadFile(`/api/v1/datasets/${openId}/files/${encodeURI(file.path)}`, auth, file.path.split("/").pop() ?? file.path).catch(showError) }}><DownloadIcon /></Button></TableCell>
          </TableRow>)}</TableBody>
        </Table>}
      </DialogContent>
    </Dialog>
  </div>
}
