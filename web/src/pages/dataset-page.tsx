import { useMemo } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  createColumnHelper,
  createPaginatedRowModel,
  rowPaginationFeature,
  tableFeatures,
  useTable,
} from "@tanstack/react-table"
import { ArrowLeftIcon, DownloadIcon, FlameIcon } from "lucide-react"
import { Link, useParams } from "react-router-dom"
import { toast } from "sonner"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { downloadFile, request, type AuthSdk } from "../lib/api"
import { formatBytes, formatRange, formatTime, showError } from "../lib/format"
import { cadenceLabel, granularityLabel, type Copy } from "../lib/i18n"
import { Pager } from "../components/pager"
import { SchemaSummary } from "../components/schema-editor"
import type { ColdFile, DatasetView, Flush, FlushResult, Manifest, TableView } from "../lib/types"

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
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="m-0 text-2xl font-semibold tracking-tight">{value.name}</h1>
          <Badge variant="outline" className="font-mono text-xs">{value.id}</Badge>
          <Badge variant="secondary" className="text-xs">{cadenceLabel(value.cadence, t)} · UTC</Badge>
        </div>
        <p className="max-w-3xl text-sm text-muted-foreground">{value.description || "—"}</p>
      </div>
      {isRoot && <Button size="sm" disabled={flush.isPending} onClick={() => flush.mutate()}>
        <FlameIcon data-icon="inline-start" />{flush.isPending ? t.flushing : t.flush}
      </Button>}
    </div>

    <TableCard table={value.table} locale={locale} t={t} />

    <Card>
      <CardHeader>
        <CardTitle>{t.ingestExample}</CardTitle>
        <CardDescription>{t.ingestExampleHint}</CardDescription>
      </CardHeader>
      <CardContent>
        <pre className="overflow-x-auto rounded-lg bg-muted p-3 font-mono text-xs leading-relaxed">{ingestExample(value, t)}</pre>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>{t.coldFilesTitle}</CardTitle>
        <CardDescription>{files.length} {t.filesUnit}</CardDescription>
      </CardHeader>
      <CardContent>
        <ColdFilesTable files={files} datasetId={value.id} auth={auth} t={t} />
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
            <TableHead>{t.startedAt}</TableHead>
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

const coldFeatures = tableFeatures({
  rowPaginationFeature,
  paginatedRowModel: createPaginatedRowModel(),
})

const coldColumns = createColumnHelper<typeof coldFeatures, ColdFile>()

function ColdFilesTable({ files, datasetId, auth, t }: {
  files: ColdFile[]
  datasetId: string
  auth: AuthSdk
  t: Copy
}) {
  const columns = useMemo(() => coldColumns.columns([
    coldColumns.accessor("path", {
      header: () => t.files,
      cell: (info) => <div className="flex max-w-96 items-center gap-2">
        <span className="truncate font-mono text-xs">{info.getValue()}</span>
        <Button size="icon-sm" variant="ghost" aria-label={t.download} onClick={() => {
          const path = info.getValue()
          void downloadFile(`/api/v1/datasets/${datasetId}/files/${encodeURI(path)}`, auth, path.split("/").pop() ?? path).catch(showError)
        }}><DownloadIcon /></Button>
      </div>,
    }),
    coldColumns.accessor("partition", { header: () => t.partition, cell: (info) => <span className="font-mono text-xs text-muted-foreground">{info.getValue()}</span> }),
    coldColumns.accessor("rows", { header: () => t.rows, cell: (info) => info.getValue().toLocaleString() }),
    coldColumns.accessor("bytes", { header: () => t.bytes, cell: (info) => <span className="text-muted-foreground">{formatBytes(info.getValue())}</span> }),
  ]), [auth, datasetId, t])
  const table = useTable({ features: coldFeatures, columns, data: files, initialState: { pagination: { pageIndex: 0, pageSize: 20 } } })
  if (files.length === 0) return <p className="text-sm text-muted-foreground">{t.noColdFiles}</p>
  return <>
    <Table>
      <TableHeader>{table.getHeaderGroups().map((group) => <TableRow key={group.id}>
        {group.headers.map((header) => <TableHead key={header.id}>
          {header.isPlaceholder ? null : <table.FlexRender header={header} />}
        </TableHead>)}
      </TableRow>)}</TableHeader>
      <TableBody>{table.getRowModel().rows.map((row) => <TableRow key={row.id}>
        {row.getAllCells().map((cell) => <TableCell key={cell.id}><table.FlexRender cell={cell} /></TableCell>)}
      </TableRow>)}</TableBody>
    </Table>
    <Pager table={table} label={`${files.length} ${t.records}`} />
  </>
}

function statusLabel(entry: Flush, t: Copy) {
  if (entry.status === "completed") return t.statusCompleted
  if (entry.status === "failed") return `${t.statusFailed}: ${entry.message}`
  return t.statusRunning
}

function TableCard({ table, locale, t }: { table: TableView; locale: string; t: Copy }) {
  return <Card>
    <CardHeader>
      <div className="flex items-center justify-between gap-2">
        <CardTitle className="text-base">{t.schemaTitle}</CardTitle>
        <Badge variant="outline">{t.granularity} · {granularityLabel(table.granularity, t)}</Badge>
      </div>
      <CardDescription>
        {t.primaryKey} {table.primary_key.join(", ")} · {t.timeColumn} {table.time_column}
      </CardDescription>
    </CardHeader>
    <CardContent className="flex flex-col gap-3 text-sm">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Detail label={t.hotRows} value={table.rows.toLocaleString()} />
        <Detail label={t.coolableRows} value={table.coolable_rows.toLocaleString()} />
        <Detail label={t.coldFiles} value={String(table.cold_files)} />
        <Detail label={t.coldBytes} value={formatBytes(table.cold_bytes)} />
      </div>
      <Detail label={t.hotRange} value={formatRange(table.oldest_time, table.newest_time, locale)} />
      <Detail label={t.coldRange} value={formatRange(table.cold_from, table.cold_until, locale)} />
      <SchemaSummary table={table} t={t} />
    </CardContent>
  </Card>
}

function Detail({ label, value }: { label: string; value: string }) {
  return <div className="flex flex-col">
    <span className="text-xs text-muted-foreground">{label}</span>
    <span className="truncate text-sm">{value}</span>
  </div>
}

// A concrete example beats a link to the docs: the sample row is generated from
// the declared columns, so it is always valid for this dataset.
function ingestExample(dataset: DatasetView, t: Copy) {
  const table = dataset.table
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
  const body = JSON.stringify({ upsert: [sample] })
  return `curl -X POST "https://firma.ntnl.io/api/v1/ingest/${dataset.id}" \\
  -H "Authorization: Bearer $FIRMA_WRITE_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '${body}'

# ${t.ingestExampleRewrite}
# {"delete": [{"${table.primary_key.join("\": \"…\", \"")}": "…"}]}`
}
