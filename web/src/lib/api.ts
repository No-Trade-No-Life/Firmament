import type { AuthMiniContextValue } from "auth-mini-react-components"
import type { Copy } from "./i18n"

export type AuthSdk = NonNullable<AuthMiniContextValue["sdk"]>

/// The words a failure is phrased with, so the same error reads correctly in
/// both languages instead of leaking whatever the transport happened to say.
export type ErrorCopy = Pick<
  Copy,
  "error" | "errorNetwork" | "errorRejected" | "errorServerUnavailable" | "errorBadResponse"
>

/// A failed request, kept apart from the sentence the user ends up reading.
///
/// `detail` carries what the server said: its JSON `error` field when there is
/// one, otherwise the first characters of the raw body. The body is only ever
/// parsed behind a guard, so a `text/plain` or HTML failure (a gateway 502, say)
/// names its reason instead of surfacing as
/// `Unexpected token 'F', "Failed to "... is not valid JSON`.
export class RequestError extends Error {
  readonly kind: "network" | "status" | "body"
  readonly status?: number
  readonly detail?: string

  constructor(kind: RequestError["kind"], message: string, options: { status?: number; detail?: string } = {}) {
    super(message)
    this.name = "RequestError"
    this.kind = kind
    this.status = options.status
    this.detail = options.detail
  }
}

export function errorMessage(error: unknown, t: ErrorCopy): string {
  if (!(error instanceof RequestError)) {
    return error instanceof Error && error.message ? error.message : t.error
  }
  if (error.kind === "network") return t.errorNetwork
  const parts = [errorLabel(error, t)]
  if (error.status && error.status >= 400) parts.push(`HTTP ${error.status}`)
  if (error.detail) parts.push(error.detail)
  return parts.join(" · ")
}

function errorLabel(error: RequestError, t: ErrorCopy): string {
  if (error.kind === "body") return t.errorBadResponse
  return (error.status ?? 0) >= 500 ? t.errorServerUnavailable : t.errorRejected
}

// RECOVERY: the Browser SDK rotates access tokens without re-rendering
// consumers, so tokens are resolved when a request is sent. A stale snapshot
// gets a single retry after a session refresh; a failed refresh falls back to
// the original 401 response so callers surface the real authentication error.
async function send(path: string, sdk: AuthSdk | undefined, init?: RequestInit): Promise<Response> {
  const attempt = (accessToken?: string) =>
    fetch(path, {
      ...init,
      headers: {
        "Content-Type": "application/json",
        ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
        ...init?.headers,
      },
    })
  let response = await attempt(sdk?.session.getState().accessToken ?? undefined).catch(networkFailure)
  if (response.status === 401 && sdk) {
    const refreshed = await sdk.session.refresh().catch(() => null)
    if (refreshed?.accessToken) response = await attempt(refreshed.accessToken).catch(networkFailure)
  }
  return response
}

function networkFailure(cause: unknown): never {
  throw new RequestError("network", "network request failed", {
    detail: cause instanceof Error ? cause.message : undefined,
  })
}

function parseJson(text: string): { parsed: boolean; value: unknown } {
  try {
    return { parsed: true, value: JSON.parse(text) }
  } catch {
    return { parsed: false, value: undefined }
  }
}

/// The first readable characters of a body that is not the API's JSON envelope.
/// Markup (an HTML 502 page) is dropped: the status already says what happened.
function snippet(text: string): string | undefined {
  const trimmed = text.trim()
  if (!trimmed || trimmed.startsWith("<")) return undefined
  return trimmed.length > 200 ? `${trimmed.slice(0, 200)}…` : trimmed
}

function serverMessage(text: string): string | undefined {
  const body = parseJson(text)
  if (!body.parsed || typeof body.value !== "object" || body.value === null) return undefined
  const message = (body.value as { error?: unknown }).error
  return typeof message === "string" && message.trim() ? message : undefined
}

function refusal(response: Response, text: string): RequestError {
  return new RequestError("status", `HTTP ${response.status}`, {
    status: response.status,
    detail: serverMessage(text) ?? snippet(text),
  })
}

async function readText(response: Response): Promise<string> {
  return response.text().catch(() => "")
}

export async function request<T>(path: string, sdk?: AuthSdk, init?: RequestInit): Promise<T> {
  const response = await send(path, sdk, init)
  const text = await readText(response)
  if (!response.ok) throw refusal(response, text)
  if (!text) return undefined as T
  const body = parseJson(text)
  if (!body.parsed) {
    throw new RequestError("body", `HTTP ${response.status}`, { status: response.status, detail: snippet(text) })
  }
  return body.value as T
}

export async function requestBytes(path: string, sdk?: AuthSdk): Promise<ArrayBuffer> {
  const response = await send(path, sdk)
  if (!response.ok) throw refusal(response, await readText(response))
  return response.arrayBuffer()
}

export async function downloadFile(path: string, sdk: AuthSdk | undefined, fileName: string): Promise<void> {
  const bytes = await requestBytes(path, sdk)
  const url = URL.createObjectURL(new Blob([bytes]))
  const anchor = document.createElement("a")
  anchor.href = url
  anchor.download = fileName
  anchor.click()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}
