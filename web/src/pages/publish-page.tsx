import { useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { CheckIcon, CopyIcon, KeyRoundIcon, PlusIcon, Trash2Icon } from "lucide-react"
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
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { request, type AuthSdk } from "../lib/api"
import { formatBytes, formatTime, showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { PublishEvent, WriteToken, WriteTokenCreated } from "../lib/types"

export function PublishPage({ auth, locale, t }: { auth: AuthSdk; locale: string; t: Copy }) {
  const tokens = useQuery({ queryKey: ["write-tokens"], queryFn: () => request<WriteToken[]>("/api/v1/write-tokens", auth) })
  const events = useQuery({
    queryKey: ["publish-events"],
    queryFn: () => request<PublishEvent[]>("/api/v1/publish-events", auth),
    refetchInterval: 10000,
  })
  const [creating, setCreating] = useState(false)
  const [created, setCreated] = useState<WriteTokenCreated | null>(null)
  const [revoking, setRevoking] = useState<WriteToken | null>(null)
  const list = tokens.data ?? []
  const eventList = events.data ?? []

  return <div className="flex flex-col gap-6">
    <div>
      <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.publishTitle}</h1>
      <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{t.publishDescription}</p>
    </div>
    <Card>
      <CardHeader>
        <div className="flex items-start justify-between gap-4">
          <div className="flex flex-col gap-1.5"><CardTitle>{t.writeTokens}</CardTitle><CardDescription>{t.writeTokensDescription}</CardDescription></div>
          <Button size="sm" onClick={() => setCreating(true)}><PlusIcon data-icon="inline-start" />{t.newWriteToken}</Button>
        </div>
      </CardHeader>
      <CardContent>
        {list.length === 0 ? <Empty><EmptyHeader><EmptyMedia variant="icon"><KeyRoundIcon /></EmptyMedia><EmptyTitle>{t.noWriteTokens}</EmptyTitle><EmptyDescription>{t.writeTokensDescription}</EmptyDescription></EmptyHeader></Empty> : <Table>
          <TableHeader><TableRow><TableHead>{t.writeTokenName}</TableHead><TableHead>{t.writeTokenCreated}</TableHead><TableHead>{t.writeTokenLastUsed}</TableHead><TableHead className="text-right" /></TableRow></TableHeader>
          <TableBody>{list.map((token) => <TableRow key={token.id}>
            <TableCell className="font-medium">{token.name}</TableCell>
            <TableCell className="text-muted-foreground">{formatTime(token.created_at, locale)}</TableCell>
            <TableCell className="text-muted-foreground">{token.last_used_at ? formatTime(token.last_used_at, locale) : t.writeTokenNeverUsed}</TableCell>
            <TableCell className="text-right"><Button size="sm" variant="ghost" onClick={() => setRevoking(token)} aria-label={t.revoke}><Trash2Icon /></Button></TableCell>
          </TableRow>)}</TableBody>
        </Table>}
      </CardContent>
    </Card>
    <Card>
      <CardHeader><CardTitle>{t.publishEvents}</CardTitle><CardDescription>{t.publishEventsDescription}</CardDescription></CardHeader>
      <CardContent>
        {eventList.length === 0 ? <Empty><EmptyHeader><EmptyMedia variant="icon"><KeyRoundIcon /></EmptyMedia><EmptyTitle>{t.noPublishEvents}</EmptyTitle><EmptyDescription>{t.publishEventsDescription}</EmptyDescription></EmptyHeader></Empty> : <Table>
          <TableHeader><TableRow><TableHead>{t.publishEventTime}</TableHead><TableHead>{t.publishEventPublisher}</TableHead><TableHead>{t.publishEventDataset}</TableHead><TableHead>{t.publishEventPath}</TableHead><TableHead className="text-right">{t.statBytes}</TableHead></TableRow></TableHeader>
          <TableBody>{eventList.map((event) => <TableRow key={event.id}>
            <TableCell className="whitespace-nowrap text-muted-foreground">{formatTime(event.created_at, locale)}</TableCell>
            <TableCell>{event.token_name}</TableCell>
            <TableCell className="text-muted-foreground">{event.dataset_id}</TableCell>
            <TableCell className="max-w-72"><span className="block truncate font-mono text-xs">{event.path}</span></TableCell>
            <TableCell className="text-right text-muted-foreground">{formatBytes(event.size)}</TableCell>
          </TableRow>)}</TableBody>
        </Table>}
      </CardContent>
    </Card>
    <CreateTokenDialog auth={auth} t={t} open={creating} onOpenChange={setCreating} onCreated={setCreated} />
    <TokenSecretDialog t={t} token={created} onClose={() => setCreated(null)} />
    <RevokeTokenDialog auth={auth} t={t} token={revoking} onClose={() => setRevoking(null)} />
  </div>
}

function CreateTokenDialog({ auth, t, open, onOpenChange, onCreated }: { auth: AuthSdk; t: Copy; open: boolean; onOpenChange: (open: boolean) => void; onCreated: (token: WriteTokenCreated) => void }) {
  const queryClient = useQueryClient()
  const [name, setName] = useState("")
  const create = useMutation({
    mutationFn: () => request<WriteTokenCreated>("/api/v1/write-tokens", auth, { method: "POST", body: JSON.stringify({ name }) }),
    onSuccess: (token) => {
      toast.success(t.tokenCreated)
      void queryClient.invalidateQueries({ queryKey: ["write-tokens"] })
      setName("")
      onOpenChange(false)
      onCreated(token)
    },
    onError: showError,
  })
  return <Dialog open={open} onOpenChange={onOpenChange}>
    <DialogContent>
      <DialogHeader><DialogTitle>{t.newWriteToken}</DialogTitle><DialogDescription>{t.writeTokensDescription}</DialogDescription></DialogHeader>
      <form className="flex flex-col gap-4" onSubmit={(event) => { event.preventDefault(); create.mutate() }}>
        <FieldGroup>
          <Field><FieldLabel htmlFor="write-token-name">{t.writeTokenName}</FieldLabel><Input id="write-token-name" value={name} onChange={(event) => setName(event.target.value)} placeholder={t.writeTokenNamePlaceholder} required /></Field>
        </FieldGroup>
        <div className="flex justify-end gap-2">
          <Button type="button" variant="ghost" onClick={() => onOpenChange(false)}>{t.cancel}</Button>
          <Button type="submit" disabled={create.isPending || name.trim() === ""}>{create.isPending ? t.creating : t.create}</Button>
        </div>
      </form>
    </DialogContent>
  </Dialog>
}

function TokenSecretDialog({ t, token, onClose }: { t: Copy; token: WriteTokenCreated | null; onClose: () => void }) {
  const [copied, setCopied] = useState(false)
  return <Dialog open={token !== null} onOpenChange={(open) => { if (!open) { setCopied(false); onClose() } }}>
    <DialogContent>
      <DialogHeader><DialogTitle>{t.writeTokenSecretTitle}</DialogTitle><DialogDescription>{t.writeTokenSecretHint}</DialogDescription></DialogHeader>
      {token && <div className="flex items-center gap-2">
        <code className="min-w-0 flex-1 truncate rounded-md border bg-muted px-3 py-2 font-mono text-xs">{token.secret}</code>
        <Button size="sm" variant="outline" onClick={() => { void navigator.clipboard.writeText(token.secret); setCopied(true); toast.success(t.copied) }}>{copied ? <CheckIcon data-icon="inline-start" /> : <CopyIcon data-icon="inline-start" />}{t.copy}</Button>
      </div>}
    </DialogContent>
  </Dialog>
}

function RevokeTokenDialog({ auth, t, token, onClose }: { auth: AuthSdk; t: Copy; token: WriteToken | null; onClose: () => void }) {
  const queryClient = useQueryClient()
  const revoke = useMutation({
    mutationFn: (target: WriteToken) => request<void>(`/api/v1/write-tokens/${target.id}`, auth, { method: "DELETE" }),
    onSuccess: () => { toast.success(t.tokenRevoked); void queryClient.invalidateQueries({ queryKey: ["write-tokens"] }); onClose() },
    onError: (error) => { onClose(); showError(error as Error) },
  })
  return <AlertDialog open={token !== null} onOpenChange={(open) => { if (!open) onClose() }}>
    <AlertDialogContent>
      <AlertDialogHeader><AlertDialogTitle>{t.revokeTitle}</AlertDialogTitle><AlertDialogDescription>{t.revokeDescription}</AlertDialogDescription></AlertDialogHeader>
      <AlertDialogFooter>
        <AlertDialogCancel>{t.cancel}</AlertDialogCancel>
        <AlertDialogAction onClick={(event) => { event.preventDefault(); if (token) revoke.mutate(token) }}>{t.revoke}</AlertDialogAction>
      </AlertDialogFooter>
    </AlertDialogContent>
  </AlertDialog>
}
