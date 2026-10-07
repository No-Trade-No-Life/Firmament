import { useQuery } from "@tanstack/react-query"
import { DatabaseIcon, FileTextIcon, FolderDownIcon } from "lucide-react"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { request, type AuthSdk } from "../lib/api"
import { formatBytes, formatTime } from "../lib/format"
import type { Copy } from "../lib/i18n"
import type { Dataset } from "../lib/types"

export function OverviewPage({ auth, locale, t }: { auth: AuthSdk; locale: string; t: Copy }) {
  const datasets = useQuery({ queryKey: ["datasets"], queryFn: () => request<Dataset[]>("/api/v1/datasets", auth) })
  const list = datasets.data ?? []
  const totals = list.reduce((acc, dataset) => ({ files: acc.files + dataset.file_count, bytes: acc.bytes + dataset.bytes }), { files: 0, bytes: 0 })
  return <div className="flex flex-col gap-6">
    <div>
      <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.overviewTitle}</h1>
      <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{t.overviewSubtitle}</p>
    </div>
    <div className="grid gap-4 sm:grid-cols-3">
      <StatCard icon={DatabaseIcon} label={t.statDatasets} value={String(list.length)} />
      <StatCard icon={FileTextIcon} label={t.statFiles} value={String(totals.files)} />
      <StatCard icon={FolderDownIcon} label={t.statBytes} value={formatBytes(totals.bytes)} />
    </div>
    <div className="grid gap-4 lg:grid-cols-2">
      <Card>
        <CardHeader><CardTitle>{t.recentDatasets}</CardTitle></CardHeader>
        <CardContent>
          {list.length === 0 ? <p className="text-sm text-muted-foreground">{t.noDatasets}</p> : <Table>
            <TableHeader><TableRow><TableHead>{t.datasets}</TableHead><TableHead>{t.files}</TableHead><TableHead>{t.statBytes}</TableHead><TableHead>{t.updated}</TableHead></TableRow></TableHeader>
            <TableBody>{list.map((dataset) => <TableRow key={dataset.id}><TableCell className="font-medium">{dataset.name}</TableCell><TableCell>{dataset.file_count}</TableCell><TableCell>{formatBytes(dataset.bytes)}</TableCell><TableCell className="text-muted-foreground">{formatTime(dataset.updated_at, locale)}</TableCell></TableRow>)}</TableBody>
          </Table>}
        </CardContent>
      </Card>
      <Card>
        <CardHeader><CardTitle>{t.quickStart}</CardTitle></CardHeader>
        <CardContent className="flex flex-col gap-3 text-sm"><Step n={1} text={t.quickStart1} /><Step n={2} text={t.quickStart2} /><Step n={3} text={t.quickStart3} /></CardContent>
      </Card>
    </div>
  </div>
}

function StatCard({ icon: Icon, label, value }: { icon: typeof DatabaseIcon; label: string; value: string }) {
  return <Card><CardHeader className="flex flex-row items-center justify-between gap-2 pb-0"><CardTitle className="text-sm font-medium text-muted-foreground">{label}</CardTitle><Icon className="size-4 text-muted-foreground" /></CardHeader><CardContent><p className="text-2xl font-semibold tracking-tight">{value}</p></CardContent></Card>
}

function Step({ n, text }: { n: number; text: string }) {
  return <div className="flex items-start gap-3"><span className="grid size-6 shrink-0 place-items-center rounded-full bg-primary/10 text-xs font-semibold text-primary">{n}</span><span>{text}</span></div>
}
