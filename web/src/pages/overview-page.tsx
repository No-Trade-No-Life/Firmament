import { useQuery } from "@tanstack/react-query"
import { CloudDownloadIcon, DatabaseIcon, TableIcon, ThermometerIcon } from "lucide-react"
import { Link } from "react-router-dom"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { request, type AuthSdk } from "../lib/api"
import { formatBytes } from "../lib/format"
import { cadenceLabel } from "../lib/i18n"
import type { Copy } from "../lib/i18n"
import type { DatasetView } from "../lib/types"

export function OverviewPage({ auth, t }: { auth: AuthSdk; t: Copy }) {
  const datasets = useQuery({ queryKey: ["datasets"], queryFn: () => request<DatasetView[]>("/api/v1/datasets", auth) })
  const list = datasets.data ?? []
  const totals = list.reduce(
    (accumulator, dataset) => ({
      hot: accumulator.hot + dataset.table.rows,
      coldFiles: accumulator.coldFiles + dataset.table.cold_files,
      coldBytes: accumulator.coldBytes + dataset.table.cold_bytes,
    }),
    { hot: 0, coldFiles: 0, coldBytes: 0 },
  )

  return <div className="flex flex-col gap-6">
    <div>
      <h1 className="m-0 text-2xl font-semibold tracking-tight">{t.overviewTitle}</h1>
      <p className="mt-1 max-w-3xl text-sm text-muted-foreground">{t.overviewSubtitle}</p>
    </div>
    <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
      <StatCard icon={DatabaseIcon} label={t.statDatasets} value={String(list.length)} />
      <StatCard icon={ThermometerIcon} label={t.statHotRows} value={totals.hot.toLocaleString()} />
      <StatCard icon={TableIcon} label={t.statColdFiles} value={totals.coldFiles.toLocaleString()} />
      <StatCard icon={CloudDownloadIcon} label={t.statColdBytes} value={formatBytes(totals.coldBytes)} />
    </div>
    <div className="grid gap-4 lg:grid-cols-2">
      <Card>
        <CardHeader><CardTitle>{t.recentDatasets}</CardTitle></CardHeader>
        <CardContent>
          {list.length === 0 ? <p className="text-sm text-muted-foreground">{t.noDatasets}</p> : <Table>
            <TableHeader><TableRow>
              <TableHead>{t.datasets}</TableHead>
              <TableHead>{t.coolingTitle}</TableHead>
              <TableHead>{t.hotRows}</TableHead>
              <TableHead>{t.coldFiles}</TableHead>
              <TableHead>{t.coldBytes}</TableHead>
            </TableRow></TableHeader>
            <TableBody>{list.map((dataset) => <TableRow key={dataset.id}>
              <TableCell><Link className="font-medium hover:underline" to={`/datasets/${dataset.id}`}>{dataset.name}</Link></TableCell>
              <TableCell className="text-muted-foreground">{cadenceLabel(dataset.cooling.cadence, t)} · {dataset.cooling.timezone}</TableCell>
              <TableCell>{dataset.table.rows.toLocaleString()}</TableCell>
              <TableCell>{dataset.table.cold_files}</TableCell>
              <TableCell className="text-muted-foreground">{formatBytes(dataset.table.cold_bytes)}</TableCell>
            </TableRow>)}</TableBody>
          </Table>}
        </CardContent>
      </Card>
      <Card>
        <CardHeader><CardTitle>{t.quickStart}</CardTitle></CardHeader>
        <CardContent className="flex flex-col gap-3 text-sm">
          <Step n={1} text={t.quickStart1} />
          <Step n={2} text={t.quickStart2} />
          <Step n={3} text={t.quickStart3} />
        </CardContent>
      </Card>
    </div>
  </div>
}

function StatCard({ icon: Icon, label, value }: { icon: typeof DatabaseIcon; label: string; value: string }) {
  return <Card>
    <CardHeader className="flex flex-row items-center justify-between gap-2 pb-0">
      <CardTitle className="text-sm font-medium text-muted-foreground">{label}</CardTitle>
      <Icon className="size-4 text-muted-foreground" />
    </CardHeader>
    <CardContent><p className="text-2xl font-semibold tracking-tight">{value}</p></CardContent>
  </Card>
}

function Step({ n, text }: { n: number; text: string }) {
  return <div className="flex items-start gap-3">
    <span className="grid size-6 shrink-0 place-items-center rounded-full bg-primary/10 text-xs font-semibold text-primary">{n}</span>
    <span>{text}</span>
  </div>
}
