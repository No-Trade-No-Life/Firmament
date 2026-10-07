import { useMutation } from "@tanstack/react-query"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { request, type AuthSdk } from "../lib/api"
import { showError } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { Me } from "../lib/types"

export function SetupPage({ auth, t, onDone }: { auth: AuthSdk; t: Copy; onDone: () => void }) {
  const mutation = useMutation({
    mutationFn: () => request<Me>("/api/v1/setup", auth, { method: "POST" }),
    onSuccess: () => { toast.success(t.setupDone); onDone() },
    onError: showError,
  })
  return <div className="mx-auto max-w-xl p-6"><Card><CardHeader><CardTitle>{t.setupTitle}</CardTitle><CardDescription>{t.setupDescription}</CardDescription></CardHeader><CardContent><Button disabled={mutation.isPending} onClick={() => mutation.mutate()}>{t.setupButton}</Button></CardContent></Card></div>
}
