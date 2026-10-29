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
import { request, type AuthSdk } from "../lib/api"
import { formatBytes, showError } from "../lib/format"
import { cadenceLabel, granularityLabel, type Copy } from "../lib/i18n"
import { SchemaEditor } from "../components/schema-editor"
import { buildTableSpec, schemaProblem, starterColumns, type ColumnDraft } from "../lib/schema"
import type { CoolingCadence, DatasetView, FlushResult, Granularity } from "../lib/types"

export function DatasetsPage({ auth, t, isRoot }: { auth: AuthSdk; t: Copy; isRoot: boolean }) {
  const queryClient = useQueryClient()
  const datasets = useQuery({ queryKey: ["datasets"], queryFn: () => request<DatasetView[]>("/api/v1/datasets", auth) })
  const [creating, setCreating] = useState(false)
  const [id, setId] = useState("")
  const [name, setName] = useState("")
  const [description, setDescription] = useState("")
  const [cadence, setCadence] = useState<CoolingCadence>("daily")
  const [granularity, setGranularity] = useState<Granularity>("day")
  const [columns, setColumns] = useState<ColumnDraft[]>(starterColumns)
  const problem = schemaProblem(columns, t)
  const list = datasets.data ?? []

  const create = useMutation({
    mutationFn: () => request<DatasetView>("/api/v1/datasets", auth, {
      method: "POST",
      body: JSON.stringify({ id, name, description, cadence, table: buildTableSpec(columns, granularity) }),
    }),
    onSuccess: () => {
      setCreating(false)
      setId("")
      setName("")
      setDescription("")
      setGranularity("day")
      setColumns(starterColumns())
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
        <CardContent className="flex flex-1 flex-col gap-3">
          <div className="flex flex-wrap gap-1">
            <Badge variant="secondary" className="text-xs">{t.granularity} · {granularityLabel(dataset.table.granularity, t)}</Badge>
          </div>
          <div className="text-xs text-muted-foreground">
            <p>{t.coolingTitle} {cadenceLabel(dataset.cadence, t)} · UTC</p>
            <p>{t.hotRows} {dataset.table.rows.toLocaleString()}{dataset.table.coolable_rows > 0 ? ` · ${t.coolableRows} ${dataset.table.coolable_rows.toLocaleString()}` : ""}</p>
            <p>{t.coldFiles} {dataset.table.cold_files} · {t.coldBytes} {formatBytes(dataset.table.cold_bytes)}</p>
          </div>
          <div className="mt-auto flex flex-wrap gap-2">
            <Link to={`/datasets/${dataset.id}`} className={buttonVariants({ variant: "outline", size: "sm" })}>{t.openDataset}</Link>
            {isRoot && <FlushButton auth={auth} dataset={dataset} t={t} />}
          </div>
        </CardContent>
      </Card>)}
    </div>}

    <Dialog open={creating} onOpenChange={setCreating}>
      <DialogContent className="sm:max-w-3xl">
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
            <div className="grid gap-4 sm:grid-cols-2">
              <Field>
                <FieldLabel>{t.cadence}</FieldLabel>
                <Select value={cadence} onValueChange={(value) => setCadence(value as CoolingCadence)}>
                  <SelectTrigger className="w-full"><SelectValue>{(value) => cadenceLabel(value as CoolingCadence, t)}</SelectValue></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="daily">{t.cadenceDaily}</SelectItem>
                    <SelectItem value="weekly">{t.cadenceWeekly}</SelectItem>
                    <SelectItem value="monthly">{t.cadenceMonthly}</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field>
                <FieldLabel>{t.granularity}</FieldLabel>
                <Select value={granularity} onValueChange={(value) => setGranularity(value as Granularity)}>
                  <SelectTrigger className="w-full"><SelectValue>{(value) => granularityLabel(value as Granularity, t)}</SelectValue></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="hour">{t.granularityHour}</SelectItem>
                    <SelectItem value="day">{t.granularityDay}</SelectItem>
                    <SelectItem value="month">{t.granularityMonth}</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
            </div>
            <FieldDescription>{t.coolingHint}</FieldDescription>
            <FieldDescription>{t.granularityHint}</FieldDescription>
            <Field>
              <FieldLabel>{t.schemaTitle}</FieldLabel>
              <SchemaEditor columns={columns} onChange={setColumns} t={t} />
            </Field>
          </FieldGroup>
          <div className="flex justify-end gap-2">
            <Button type="button" variant="ghost" onClick={() => setCreating(false)}>{t.cancel}</Button>
            <Button type="submit" disabled={create.isPending || problem !== null}>{create.isPending ? t.creating : t.create}</Button>
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
