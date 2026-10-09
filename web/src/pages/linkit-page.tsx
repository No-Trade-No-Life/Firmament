import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { request, type AuthSdk } from "../lib/api"
import { formatTime, showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { LinkitStatus } from "../lib/types"

export function LinkitPage({ auth, locale, t }: { auth: AuthSdk; locale: string; t: Copy }) {
  const client = useQueryClient()
  const status = useQuery({ queryKey: ["linkit"], queryFn: () => request<LinkitStatus>("/api/v1/linkit", auth), refetchInterval: 10_000 })
  const ensure = useMutation({
    mutationFn: () => request<LinkitStatus>("/api/v1/linkit", auth, { method: "POST" }),
    onSuccess: (value) => client.setQueryData(["linkit"], value),
    onError: showError,
  })
  const test = useMutation({
    mutationFn: () => request<{ sent: boolean }>("/api/v1/linkit/test", auth, { method: "POST" }),
    onSuccess: () => toast.success(t.linkitTestSent),
    onError: showError,
    onSettled: () => void client.invalidateQueries({ queryKey: ["linkit"] }),
  })
  const busy = ensure.isPending || test.isPending
  const data = status.data
  return <div className="flex flex-col gap-6">
    <div>
      <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.linkitTitle}</h1>
      <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{t.linkitDescription}</p>
    </div>
    <Card className="max-w-2xl">
      <CardHeader>
        <CardTitle>{t.linkitStatusTitle}</CardTitle>
        <CardDescription>{t.linkitStatusDescription}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {data && <>
          <div className="flex flex-wrap items-center gap-3">
            <Badge variant="outline">{data.configured ? t.linkitConnected : t.linkitIncomplete}</Badge>
            <span className="text-sm">{t.linkitRecipient}: {data.recipient_username ? `@${data.recipient_username}` : "—"}</span>
          </div>
          {data.bot_id && <p className="break-all text-xs text-muted-foreground">{t.linkitBot}: <code>{data.bot_id}</code></p>}
          {data.last_attempt_at === null ? <p className="text-sm text-muted-foreground">{t.linkitEmpty}</p> : <dl className="grid gap-2 text-sm sm:grid-cols-2">
            <div><dt className="text-muted-foreground">{t.linkitLastAttempt}</dt><dd>{formatTime(data.last_attempt_at, locale)}</dd></div>
            <div><dt className="text-muted-foreground">{t.linkitLastSuccess}</dt><dd>{formatTime(data.last_success_at, locale)}</dd></div>
          </dl>}
          {data.last_error && <p role="alert" className="break-words text-sm text-destructive">{t.linkitLastError}: {data.last_error}</p>}
        </>}
        {status.error && <p role="alert" className="break-words text-sm text-destructive">{status.error.message}</p>}
        <div className="flex flex-wrap gap-2">
          {data && !data.configured && <Button variant="outline" disabled={busy} onClick={() => ensure.mutate()}>{t.linkitRepair}</Button>}
          <Button variant="outline" disabled={busy || !data?.configured} onClick={() => test.mutate()}>{t.linkitTest}</Button>
        </div>
        <p className="text-xs text-muted-foreground">{t.linkitBoundary}</p>
      </CardContent>
    </Card>
  </div>
}
