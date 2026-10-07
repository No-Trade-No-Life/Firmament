import { useEffect, useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Field, FieldDescription, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { request, type AuthSdk } from "../lib/api"
import { showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { LinkitSettings } from "../lib/types"

export function LinkitPage({ auth, t }: { auth: AuthSdk; t: Copy }) {
  const queryClient = useQueryClient()
  const settings = useQuery({ queryKey: ["linkit"], queryFn: () => request<LinkitSettings>("/api/v1/linkit", auth) })
  const [recipient, setRecipient] = useState("")
  const [botToken, setBotToken] = useState("")
  useEffect(() => { setRecipient(settings.data?.recipient_username ?? "") }, [settings.data])
  const save = useMutation({
    mutationFn: () => request<LinkitSettings>("/api/v1/linkit", auth, { method: "PUT", body: JSON.stringify({ recipient_username: recipient, bot_token: botToken }) }),
    onSuccess: () => { toast.success(t.save); setBotToken(""); void queryClient.invalidateQueries({ queryKey: ["linkit"] }) },
    onError: showError,
  })
  const test = useMutation({
    mutationFn: () => request<{ sent: boolean }>("/api/v1/linkit/test", auth, { method: "POST" }),
    onSuccess: () => toast.success(t.linkitTestSent),
    onError: showError,
  })
  return <div className="flex flex-col gap-6">
    <div>
      <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.linkitTitle}</h1>
      <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{t.linkitDescription}</p>
    </div>
    <Card className="max-w-2xl">
      <CardHeader>
        <CardTitle>{settings.data ? t.linkitConfigured : t.linkitNotConfigured}</CardTitle>
        <CardDescription>Linkit Bot API</CardDescription>
      </CardHeader>
      <CardContent>
        <form className="flex flex-col gap-5" onSubmit={(event) => { event.preventDefault(); save.mutate() }}>
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="linkit-recipient">{t.linkitUsername}</FieldLabel>
              <Input id="linkit-recipient" value={recipient} onChange={(event) => setRecipient(event.target.value)} placeholder="alice" required />
            </Field>
            <Field>
              <FieldLabel htmlFor="linkit-token">{t.linkitToken}</FieldLabel>
              <Input id="linkit-token" type="password" value={botToken} onChange={(event) => setBotToken(event.target.value)} placeholder="sk-…" required />
              <FieldDescription>{t.linkitDescription}</FieldDescription>
            </Field>
          </FieldGroup>
          <div className="flex flex-wrap gap-2">
            <Button type="submit" disabled={save.isPending}>{save.isPending ? t.saving : t.save}</Button>
            <Button type="button" variant="outline" disabled={!settings.data || test.isPending} onClick={() => test.mutate()}>{t.linkitTest}</Button>
          </div>
        </form>
      </CardContent>
    </Card>
  </div>
}
