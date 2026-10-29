import { useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { FlameIcon, PlusIcon } from "lucide-react"
import { Link } from "react-router-dom"
import { toast } from "sonner"

import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Field, FieldDescription, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { request, type AuthSdk } from "../lib/api"
import { formatBytes, formatWindow, showError } from "../lib/format"
import { granularityLabel, type Copy } from "../lib/i18n"
import type { DatasetView, FlushResult, TableSpec } from "../lib/types"

const TABLE_TEMPLATE = `[
  {
    "name": "bars",
    "primary_key": ["symbol", "updated_at"],
    "time_column": "updated_at",
    "granularity": "day",
    "columns": [
      { "name": "symbol", "type": "text" },
      { "name": "updated_at", "type": "timestamp" },
      { "name": "open", "type": "real" },
      { "name": "high", "type": "real" },
      { "name": "low", "type": "real" },
      { "name": "close", "type": "real" },
      { "name": "volume", "type": "real" }
    ]
  }
]`

export function DatasetsPage({ auth, t, isRoot }: { auth: AuthSdk; t: Copy; isRoot: boolean }) {
  const queryClient = useQueryClient()
  const datasets = useQuery({ queryKey: ["datasets"], queryFn: () => request<DatasetView[]>("/api/v1/datasets", auth) })
  const [creating, setCreating] = useState(false)
  const [id, setId] = useState("")
  const [name, setName] = useState("")
  const [description, setDescription] = useState("")
  const [windowValue, setWindowValue] = useState("7")
  const [windowUnit, setWindowUnit] = useState<"days" | "hours">("days")
  const [tables, setTables] = useState(TABLE_TEMPLATE)
  const list = datasets.data ?? []

  const create = useMutation({
    mutationFn: () => {
      const parsed = JSON.parse(tables) as TableSpec[]
      const hotWindowSeconds = Number(windowValue) * (windowUnit === "days" ? 86_400 : 3_600)
      return request<DatasetView>("/api/v1/datasets", auth, {
        method: "POST",
        body: JSON.stringify({ id, name, description, hot_window_seconds: hotWindowSeconds, tables: parsed }),
      })
    },
    onSuccess: () => {
      setCreating(false)
      setId("")
      setName("")
      setDescription("")
      setTables(TABLE_TEMPLATE)
      toast.success(t.datasetCreated)
      void queryClient.invalidateQueries({ queryKey: ["datasets"] })
    },
    onError: (error) => showError(error instanceof Error ? error : new Error(String(error))),
  })

  return <div className="flex flex-col gap-6">
    <div className="flex items-start justify-between gap-4">
      <div>
        <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.datasetsTitle}</h1>
        <p className="mt-1 max-w-3xl text-sm text-muted-foreground">{t.datasetsDescription}</p>
      </div>
      {isRoot && <Button size="sm" onClick={() => setCreating(true)}><PlusIcon data-icon="inline-start" />{t.newDataset}</Button>}
    </div>

    {list.length === 0 ? <p className="text-sm text-muted-foreground">{t.noDatasets}</p> : <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
      {list.map((dataset) => <Card key={dataset.id}>
        <CardHeader>
          <div className="flex items-start justify-between gap-2">
            <CardTitle>{dataset.name}</CardTitle>
            <Badge variant="outline" className="font-mono text-xs">{dataset.id}</Badge>
          </div>
          <CardDescription>{dataset.description || "—"}</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <div className="flex flex-wrap gap-1">
            {dataset.tables.map((table) => <Badge key={table.name} variant="secondary" className="font-mono text-xs">
              {table.name} · {granularityLabel(table.granularity, t)}
            </Badge>)}
          </div>
          <p className="text-xs text-muted-foreground">
            {t.hotWindow} {formatWindow(dataset.hot_window_seconds)} · {t.hotRows} {dataset.hot_rows.toLocaleString()}
            {dataset.coolable_rows > 0 ? ` · ${t.coolableRows} ${dataset.coolable_rows.toLocaleString()}` : ""}
            {" · "}{t.coldFiles} {dataset.cold_files} · {formatBytes(dataset.cold_bytes)}
          </p>
          <div className="flex flex-wrap gap-2">
            <Link to={`/datasets/${dataset.id}`} className={buttonVariants({ variant: "outline", size: "sm" })}>{t.openDataset}</Link>
            {isRoot && <FlushButton auth={auth} dataset={dataset} t={t} />}
          </div>
        </CardContent>
      </Card>)}
    </div>}

    <Dialog open={creating} onOpenChange={setCreating}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>{t.newDataset}</DialogTitle>
          <DialogDescription>{t.datasetsDescription}</DialogDescription>
        </DialogHeader>
        <form className="flex flex-col gap-4" onSubmit={(event) => { event.preventDefault(); create.mutate() }}>
          <FieldGroup>
            <div className="grid gap-4 sm:grid-cols-2">
              <Field>
                <FieldLabel htmlFor="dataset-id">{t.datasetId}</FieldLabel>
                <Input id="dataset-id" value={id} onChange={(event) => setId(event.target.value)} placeholder={t.datasetIdPlaceholder} required />
                <FieldDescription>{t.datasetIdHint}</FieldDescription>
              </Field>
              <Field>
                <FieldLabel htmlFor="dataset-name">{t.datasetNameField}</FieldLabel>
                <Input id="dataset-name" value={name} onChange={(event) => setName(event.target.value)} required />
              </Field>
            </div>
            <Field>
              <FieldLabel htmlFor="dataset-description">{t.datasetDescriptionField}</FieldLabel>
              <Input id="dataset-description" value={description} onChange={(event) => setDescription(event.target.value)} />
            </Field>
            <Field>
              <FieldLabel>{t.hotWindow}</FieldLabel>
              <div className="flex gap-2">
                <Input className="max-w-24" type="number" min="0" value={windowValue} onChange={(event) => setWindowValue(event.target.value)} />
                <Select value={windowUnit} onValueChange={(value) => setWindowUnit(value as "days" | "hours")}>
                  <SelectTrigger className="max-w-32"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="days">{t.windowDays}</SelectItem>
                    <SelectItem value="hours">{t.windowHours}</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <FieldDescription>{t.hotWindowHint}</FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor="dataset-tables">{t.tablesJson}</FieldLabel>
              <Textarea id="dataset-tables" className="min-h-64 font-mono text-xs" value={tables} onChange={(event) => setTables(event.target.value)} spellCheck={false} />
              <FieldDescription>{t.tablesJsonHint}</FieldDescription>
            </Field>
          </FieldGroup>
          <div className="flex justify-end gap-2">
            <Button type="button" variant="ghost" onClick={() => setCreating(false)}>{t.cancel}</Button>
            <Button type="submit" disabled={create.isPending}>{create.isPending ? t.creating : t.create}</Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  </div>
}

function FlushButton({ auth, dataset, t }: { auth: AuthSdk; dataset: DatasetView; t: Copy }) {
  const queryClient = useQueryClient()
  const flush = useMutation({
    mutationFn: () => request<FlushResult>(`/api/v1/datasets/${dataset.id}/flushes`, auth, { method: "POST" }),
    onSuccess: (result) => {
      toast.success(`${t.flushed} · batch ${result.batch} · ${result.rows} ${t.rows}`)
      void queryClient.invalidateQueries()
    },
    onError: (error) => showError(error instanceof Error ? error : new Error(String(error))),
  })
  return <Button size="sm" variant="ghost" disabled={flush.isPending} onClick={() => flush.mutate()}>
    <FlameIcon data-icon="inline-start" />{flush.isPending ? t.flushing : t.flush}
  </Button>
}
