import { useMemo, useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { CopyIcon, PlusIcon, Trash2Icon } from "lucide-react"
import {
  createColumnHelper,
  createPaginatedRowModel,
  rowPaginationFeature,
  tableFeatures,
  useTable,
} from "@tanstack/react-table"
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
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Pager } from "../components/pager"
import { request, type AuthSdk } from "../lib/api"
import { formatTime, showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { IngestEvent, WriteToken, WriteTokenCreated } from "../lib/types"

const auditFeatures = tableFeatures({
  rowPaginationFeature,
  paginatedRowModel: createPaginatedRowModel(),
})

const auditColumns = createColumnHelper<typeof auditFeatures, IngestEvent>()

function IngestEventsTable({ events, locale, t }: { events: IngestEvent[]; locale: string; t: Copy }) {
  const columns = useMemo(() => auditColumns.columns([
    auditColumns.accessor("created_at", { header: () => t.ingestEventTime, cell: (info) => formatTime(info.getValue(), locale) }),
    auditColumns.accessor("token_name", { header: () => t.ingestEventPublisher }),
    auditColumns.accessor("dataset_id", { header: () => t.ingestEventDataset, cell: (info) => <span className="font-mono text-xs">{info.getValue()}</span> }),
    auditColumns.accessor("upserted", { header: () => t.ingestEventUpserts }),
    auditColumns.accessor("deleted", { header: () => t.ingestEventDeletes }),
  ]), [t, locale])
  const table = useTable({ features: auditFeatures, columns, data: events, initialState: { pagination: { pageIndex: 0, pageSize: 10 } } })
  if (events.length === 0) return <p className="text-sm text-muted-foreground">{t.noIngestEvents}</p>
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
    <Pager table={table} label={`${events.length} ${t.records}`} />
  </>
}

export function IngestPage({ auth, locale, t }: { auth: AuthSdk; locale: string; t: Copy }) {
  const queryClient = useQueryClient()
  const tokens = useQuery({ queryKey: ["write-tokens"], queryFn: () => request<WriteToken[]>("/api/v1/write-tokens", auth) })
  const events = useQuery({ queryKey: ["ingest-events"], queryFn: () => request<IngestEvent[]>("/api/v1/ingest-events", auth) })
  const [name, setName] = useState("")
  const [created, setCreated] = useState<WriteTokenCreated | null>(null)
  const [revoking, setRevoking] = useState<WriteToken | null>(null)

  const create = useMutation({
    mutationFn: () => request<WriteTokenCreated>("/api/v1/write-tokens", auth, { method: "POST", body: JSON.stringify({ name }) }),
    onSuccess: (token) => {
      setName("")
      setCreated(token)
      toast.success(t.tokenCreated)
      void queryClient.invalidateQueries({ queryKey: ["write-tokens"] })
    },
    onError: showError,
  })
  const revoke = useMutation({
    mutationFn: (token: WriteToken) => request<void>(`/api/v1/write-tokens/${token.id}`, auth, { method: "DELETE" }),
    onSuccess: () => {
      setRevoking(null)
      toast.success(t.tokenRevoked)
      void queryClient.invalidateQueries({ queryKey: ["write-tokens"] })
    },
    onError: showError,
  })

  return <div className="flex flex-col gap-6">
    <div>
      <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.ingestTitle}</h1>
      <p className="mt-1 max-w-3xl text-sm text-muted-foreground">{t.ingestDescription}</p>
    </div>

    <Card>
      <CardHeader>
        <CardTitle>{t.writeTokens}</CardTitle>
        <CardDescription>{t.writeTokensDescription}</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <form className="flex flex-wrap items-end gap-2" onSubmit={(event) => { event.preventDefault(); create.mutate() }}>
          <FieldGroup className="flex-1">
            <Field>
              <FieldLabel htmlFor="token-name">{t.writeTokenName}</FieldLabel>
              <Input id="token-name" value={name} onChange={(event) => setName(event.target.value)} placeholder={t.writeTokenNamePlaceholder} required />
            </Field>
          </FieldGroup>
          <Button type="submit" disabled={create.isPending}>
            <PlusIcon data-icon="inline-start" />{create.isPending ? t.creating : t.newWriteToken}
          </Button>
        </form>
        {(tokens.data ?? []).length === 0 ? <p className="text-sm text-muted-foreground">{t.noWriteTokens}</p> : <Table>
          <TableHeader><TableRow>
            <TableHead>{t.writeTokenName}</TableHead>
            <TableHead>{t.writeTokenCreated}</TableHead>
            <TableHead>{t.writeTokenLastUsed}</TableHead>
            <TableHead className="text-right" />
          </TableRow></TableHeader>
          <TableBody>{(tokens.data ?? []).map((token) => <TableRow key={token.id}>
            <TableCell className="font-medium">{token.name}</TableCell>
            <TableCell className="text-muted-foreground">{formatTime(token.created_at, locale)}</TableCell>
            <TableCell className="text-muted-foreground">{token.last_used_at ? formatTime(token.last_used_at, locale) : t.writeTokenNeverUsed}</TableCell>
            <TableCell className="text-right">
              <Button size="icon-sm" variant="ghost" aria-label={t.revoke} onClick={() => setRevoking(token)}><Trash2Icon /></Button>
            </TableCell>
          </TableRow>)}</TableBody>
        </Table>}
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>{t.ingestEvents}</CardTitle>
        <CardDescription>{t.ingestEventsDescription}</CardDescription>
      </CardHeader>
      <CardContent>
        <IngestEventsTable events={events.data ?? []} locale={locale} t={t} />
      </CardContent>
    </Card>

    <Dialog open={created !== null} onOpenChange={(open) => { if (!open) setCreated(null) }}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{t.writeTokenSecretTitle}</DialogTitle>
          <DialogDescription>{t.writeTokenSecretHint}</DialogDescription>
        </DialogHeader>
        <div className="flex items-center gap-2">
          <code className="flex-1 overflow-x-auto rounded-lg bg-muted p-3 font-mono text-xs">{created?.secret ?? ""}</code>
          <Button size="icon-sm" variant="outline" aria-label={t.copy} onClick={() => {
            if (!created) return
            void navigator.clipboard.writeText(created.secret).then(() => toast.success(t.copied)).catch(showError)
          }}><CopyIcon /></Button>
        </div>
      </DialogContent>
    </Dialog>

    <AlertDialog open={revoking !== null} onOpenChange={(open) => { if (!open) setRevoking(null) }}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{t.revokeTitle}</AlertDialogTitle>
          <AlertDialogDescription>{t.revokeDescription}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>{t.cancel}</AlertDialogCancel>
          <AlertDialogAction onClick={() => { if (revoking) revoke.mutate(revoking) }}>{t.revoke}</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  </div>
}
