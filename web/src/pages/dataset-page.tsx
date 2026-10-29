import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { ArrowLeftIcon, DownloadIcon, FlameIcon } from "lucide-react"
import { Link, useParams } from "react-router-dom"
import { toast } from "sonner"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { downloadFile, request, type AuthSdk } from "../lib/api"
import { formatBytes, formatStamp, formatTime, formatWindow, showError } from "../lib/format"
import { granularityLabel, type Copy } from "../lib/i18n"
import type { ColumnSpec, DatasetView, Flush, FlushResult, Manifest, TableView } from "../lib/types"

const COLD_FILE_ROWS = 200

export function DatasetPage({ auth, locale, t, isRoot }: { auth: AuthSdk; locale: string; t: Copy; isRoot: boolean }) {
  const { id = "" } = useParams()
  const queryClient = useQueryClient()
  const dataset = useQuery({ queryKey: ["dataset", id], queryFn: () => request<DatasetView>(`/api/v1/datasets/${id}`, auth), enabled: id !== "" })
  const flushes = useQuery({ queryKey: ["flushes", id], queryFn: () => request<Flush[]>(`/api/v1/datasets/${id}/flushes`, auth), enabled: id !== "" })
  const manifest = useQuery({ queryKey: ["manifest", id, "all"], queryFn: () => request<Manifest>(`/api/v1/datasets/${id}/manifest?since=0`, auth), enabled: id !== "" })
  const flush = useMutation({
    mutationFn: () => request<FlushResult>(`/api/v1/datasets/${id}/flushes`, auth, { method: "POST" }),
    onSuccess: (result) => {
      toast.success(`${t.flushed} · batch ${result.batch} · ${result.files} ${t.files}`)
      void queryClient.invalidateQueries()
    },
    onError: (error) => showError(error instanceof Error ? error : new Error(String(error))),
  })

  if (dataset.isPending) return <p className="text-sm text-muted-foreground">…</p>
  if (dataset.error || !dataset.data) return <p className="text-sm text-muted-foreground">{dataset.error?.message ?? t.error}</p>
  const value = dataset.data
  const files = manifest.data?.files ?? []

  return <div className="flex flex-col gap-6">
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div className="flex flex-col gap-2">
        <Link to="/datasets" className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
          <ArrowLeftIcon className="size-3" />{t.back}
        </Link>
        <div className="flex items-center gap-2">
          <h1 className="m-0 text-2xl font-semibold tracking-tight">{value.name}</h1>
          <Badge variant="outline" className="font-mono text-xs">{value.id}</Badge>
        </div>
        <p className="max-w-3xl text-sm text-muted-foreground">{value.description || "—"}</p>
      </div>
      {isRoot && <Button size="sm" disabled={flush.isPending} onClick={() => flush.mutate()}>
        <FlameIcon data-icon="inline-start" />{flush.isPending ? t.flushing : t.flush}
      </Button>}
    </div>

    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <Metric label={t.hotWindow} value={formatWindow(value.hot_window_seconds)} />
      <Metric label={t.hotRows} value={value.hot_rows.toLocaleString()} />
      <Metric label={t.coldFiles} value={String(value.cold_files)} />
      <Metric label={t.coldBytes} value={formatBytes(value.cold_bytes)} />
    </div>

    <div className="grid gap-4 lg:grid-cols-2">
      {value.tables.map((table) => <TableCard key={table.name} table={table} locale={locale} t={t} />)}
    </div>

    <Card>
      <CardHeader>
        <CardTitle>{t.ingestExample}</CardTitle>
        <CardDescription>{t.ingestExampleHint}</CardDescription>
      </CardHeader>
      <CardContent>
        <pre className="overflow-x-auto rounded-lg bg-muted p-3 font-mono text-xs leading-relaxed">{ingestExample(value)}</pre>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>{t.coldFilesTitle}</CardTitle>
        <CardDescription>{files.length > COLD_FILE_ROWS ? `${COLD_FILE_ROWS} / ${files.length}` : `${files.length}`}</CardDescription>
      </CardHeader>
      <CardContent>
        {files.length === 0 ? <p className="text-sm text-muted-foreground">{t.noColdFiles}</p> : <Table>
          <TableHeader><TableRow>
            <TableHead>{t.tableName}</TableHead>
            <TableHead>{t.granularity}</TableHead>
            <TableHead>{t.rows}</TableHead>
            <TableHead>{t.bytes}</TableHead>
            <TableHead className="text-right" />
          </TableRow></TableHeader>
          <TableBody>{files.slice(0, COLD_FILE_ROWS).map((file) => <TableRow key={file.path}>
            <TableCell className="max-w-72 truncate font-mono text-xs">{file.path}</TableCell>
            <TableCell className="text-muted-foreground">{formatStamp(file.min_time, locale)}</TableCell>
            <TableCell>{file.rows.toLocaleString()}</TableCell>
            <TableCell className="text-muted-foreground">{formatBytes(file.bytes)}</TableCell>
            <TableCell className="text-right">
              <Button size="icon-sm" variant="ghost" aria-label={t.download} onClick={() => {
                void downloadFile(`/api/v1/datasets/${value.id}/files/${encodeURI(file.path)}`, auth, file.path.split("/").pop() ?? file.path).catch(showError)
              }}><DownloadIcon /></Button>
            </TableCell>
          </TableRow>)}</TableBody>
        </Table>}
      </CardContent>
    </Card>

    <Card>
      <CardHeader><CardTitle>{t.flushHistory}</CardTitle></CardHeader>
      <CardContent>
        {(flushes.data ?? []).length === 0 ? <p className="text-sm text-muted-foreground">{t.noFlushes}</p> : <Table>
          <TableHeader><TableRow>
            <TableHead>{t.batch}</TableHead>
            <TableHead>{t.status}</TableHead>
            <TableHead>{t.trigger}</TableHead>
            <TableHead>{t.files}</TableHead>
            <TableHead>{t.rows}</TableHead>
            <TableHead>{t.bytes}</TableHead>
            <TableHead>{t.hotWindow}</TableHead>
          </TableRow></TableHeader>
          <TableBody>{(flushes.data ?? []).map((entry) => <TableRow key={entry.id}>
            <TableCell className="font-mono text-xs">{entry.batch}</TableCell>
            <TableCell><Badge variant={entry.status === "failed" ? "destructive" : entry.status === "completed" ? "secondary" : "outline"}>
              {statusLabel(entry, t)}
            </Badge></TableCell>
            <TableCell className="text-muted-foreground">{entry.trigger}</TableCell>
            <TableCell>{entry.files}</TableCell>
            <TableCell>{entry.rows.toLocaleString()}</TableCell>
            <TableCell className="text-muted-foreground">{formatBytes(entry.bytes)}</TableCell>
            <TableCell className="text-muted-foreground">{formatTime(entry.created_at, locale)}</TableCell>
          </TableRow>)}</TableBody>
        </Table>}
      </CardContent>
    </Card>
  </div>
}

function statusLabel(entry: Flush, t: Copy) {
  if (entry.status === "completed") return t.statusCompleted
  if (entry.status === "failed") return `${t.statusFailed}: ${entry.message}`
  return t.statusRunning
}

function Metric({ label, value }: { label: string; value: string }) {
  return <Card size="sm">
    <CardHeader className="pb-0"><CardTitle className="text-sm font-medium text-muted-foreground">{label}</CardTitle></CardHeader>
    <CardContent><p className="text-xl font-semibold tracking-tight">{value}</p></CardContent>
  </Card>
}

function TableCard({ table, locale, t }: { table: TableView; locale: string; t: Copy }) {
  return <Card>
    <CardHeader>
      <div className="flex items-center justify-between gap-2">
        <CardTitle className="font-mono text-base">{table.name}</CardTitle>
        <Badge variant="outline">{granularityLabel(table.granularity, t)}</Badge>
      </div>
      <CardDescription>
        {t.primaryKey} {table.primary_key.join(", ")} · {t.timeColumn} {table.time_column}
      </CardDescription>
    </CardHeader>
    <CardContent className="flex flex-col gap-3 text-sm">
      <div className="grid grid-cols-2 gap-2">
        <Detail label={t.hotRows} value={table.rows.toLocaleString()} />
        <Detail label={t.coolableRows} value={table.coolable_rows.toLocaleString()} />
        <Detail label={t.coldFiles} value={String(table.cold_files)} />
        <Detail label={t.coldBytes} value={formatBytes(table.cold_bytes)} />
        <Detail label={t.hotRange} value={`${formatStamp(table.oldest_time, locale)} → ${formatStamp(table.newest_time, locale)}`} />
        <Detail label={t.coldRange} value={`${formatStamp(table.cold_from, locale)} → ${formatStamp(table.cold_until, locale)}`} />
      </div>
      <div className="flex flex-wrap gap-1">
        {table.columns.map((column) => <Badge key={column.name} variant="secondary" className="font-mono text-xs">
          {columnLabel(column)}
        </Badge>)}
      </div>
    </CardContent>
  </Card>
}

function Detail({ label, value }: { label: string; value: string }) {
  return <div className="flex flex-col">
    <span className="text-xs text-muted-foreground">{label}</span>
    <span className="truncate text-sm">{value}</span>
  </div>
}

function columnLabel(column: ColumnSpec) {
  return `${column.name}:${column.type}${column.nullable ? "?" : ""}`
}

// A concrete example beats a link to the docs: the sample row is generated from
// the declared columns, so it is always valid for this dataset.
function ingestExample(dataset: DatasetView) {
  const table = dataset.tables[0]
  if (!table) return ""
  const sample: Record<string, unknown> = {}
  for (const column of table.columns) {
    sample[column.name] = column.type === "timestamp"
      ? "2026-10-01T00:00:00Z"
      : column.type === "integer"
        ? 1
        : column.type === "real"
          ? 1.5
          : column.type === "boolean"
            ? true
            : `value`
  }
  const body = JSON.stringify({ upsert: [sample] }, null, 2)
  return `curl -X POST "https://firma.ntnl.io/api/v1/ingest/${dataset.id}/${table.name}" \\
  -H "Authorization: Bearer $FIRMA_WRITE_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '${body}'

# 同一个主键再写一次即覆盖；热窗口内还可以删除：
# {"delete": [{"${table.primary_key.join("\": \"…\", \"")}": "…"}]}`
}
