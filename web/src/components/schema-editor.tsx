import { CheckIcon, MinusIcon, PlusIcon, Trash2Icon } from "lucide-react"

import { Button } from "@/components/ui/button"
import { FieldDescription, FieldError } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { columnTypeLabel, type Copy } from "../lib/i18n"
import { blankColumn, COLUMN_TYPES, schemaProblem, type ColumnDraft } from "../lib/schema"
import type { ColumnType, TableView } from "../lib/types"

function Mark({ on = false }: { on?: boolean }) {
  return on
    ? <CheckIcon className="mx-auto size-3.5" />
    : <MinusIcon className="mx-auto size-3.5 text-muted-foreground/50" />
}

export function SchemaEditor({ columns, onChange, t }: { columns: ColumnDraft[]; onChange: (columns: ColumnDraft[]) => void; t: Copy }) {
  const replace = (index: number, patch: Partial<ColumnDraft>) =>
    onChange(columns.map((column, at) => (at === index ? { ...column, ...patch } : column)))

  // ASSUMPTION: only one column can be the time axis, and the server requires
  // the primary key to contain it, so turning it on also claims that row's key
  // and drops nullability. If a writer later reads the schema, every accepted
  // table still has exactly one time column inside its key.
  const toggleTime = (index: number, on: boolean) =>
    onChange(columns.map((column, at) => ({
      ...column,
      timeColumn: at === index ? on : false,
      primaryKey: at === index && on ? true : column.primaryKey,
      nullable: at === index && on ? false : column.nullable,
    })))

  return <div className="flex flex-col gap-2">
    {/* The table caps its own height so a wide table never pushes the error
        text and the create button off screen: 64 columns scroll here. */}
    <div className="max-h-80 overflow-y-auto rounded-md border">
      <Table className="table-fixed">
        <TableHeader className="sticky top-0 z-10 bg-popover">
          <TableRow>
            <TableHead className="h-8">{t.columnName}</TableHead>
            <TableHead className="h-8 w-32">{t.columnType}</TableHead>
            <TableHead className="h-8 w-24 text-center">{t.primaryKey}</TableHead>
            <TableHead className="h-8 w-24 text-center">{t.timeColumn}</TableHead>
            <TableHead className="h-8 w-20 text-center">{t.nullable}</TableHead>
            <TableHead className="h-8 w-10" />
          </TableRow>
        </TableHeader>
        <TableBody>{columns.map((column, index) => <TableRow key={index}>
          <TableCell className="p-1.5">
            <Input className="h-8 w-full font-mono text-xs" spellCheck={false} aria-label={t.columnName} value={column.name} onChange={(event) => replace(index, { name: event.target.value })} />
          </TableCell>
          <TableCell className="p-1.5">
            <Select value={column.type} onValueChange={(value) => replace(index, { type: value as ColumnType, ...(value === "timestamp" ? {} : { timeColumn: false }) })}>
              <SelectTrigger className="w-full" aria-label={t.columnType}><SelectValue>{(value) => columnTypeLabel(value as ColumnType, t)}</SelectValue></SelectTrigger>
              <SelectContent>{COLUMN_TYPES.map((type) => <SelectItem key={type} value={type}>{columnTypeLabel(type, t)}</SelectItem>)}</SelectContent>
            </Select>
          </TableCell>
          <TableCell className="p-1.5 text-center">{column.timeColumn
            ? <Mark on />
            : <Switch aria-label={t.primaryKey} checked={column.primaryKey} onCheckedChange={(checked) => replace(index, { primaryKey: checked })} />}
          </TableCell>
          <TableCell className="p-1.5 text-center">
            <Switch aria-label={t.timeColumn} checked={column.timeColumn} disabled={column.type !== "timestamp"} onCheckedChange={(checked) => toggleTime(index, checked)} />
          </TableCell>
          <TableCell className="p-1.5 text-center">{column.timeColumn
            ? <Mark />
            : <Switch aria-label={t.nullable} checked={column.nullable} onCheckedChange={(checked) => replace(index, { nullable: checked })} />}
          </TableCell>
          <TableCell className="p-1.5">
            <Button type="button" size="icon-xs" variant="ghost" aria-label={t.removeColumn} onClick={() => onChange(columns.filter((_, at) => at !== index))}><Trash2Icon /></Button>
          </TableCell>
        </TableRow>)}</TableBody>
      </Table>
    </div>
    <div>
      <Button type="button" size="sm" variant="outline" onClick={() => onChange([...columns, blankColumn()])}><PlusIcon data-icon="inline-start" />{t.addColumn}</Button>
    </div>
    <FieldDescription>{t.schemaEditorHint}</FieldDescription>
    <FieldError>{schemaProblem(columns, t)}</FieldError>
  </div>
}

export function SchemaSummary({ table, t }: { table: TableView; t: Copy }) {
  return <div className="overflow-hidden rounded-md border">
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>{t.columnName}</TableHead>
          <TableHead className="w-32">{t.columnType}</TableHead>
          <TableHead className="w-24 text-center">{t.primaryKey}</TableHead>
          <TableHead className="w-24 text-center">{t.timeColumn}</TableHead>
          <TableHead className="w-20 text-center">{t.nullable}</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>{table.columns.map((column) => <TableRow key={column.name}>
        <TableCell className="font-mono text-xs">{column.name}</TableCell>
        <TableCell className="text-xs">{columnTypeLabel(column.type, t)}</TableCell>
        <TableCell><Mark on={table.primary_key.includes(column.name)} /></TableCell>
        <TableCell><Mark on={table.time_column === column.name} /></TableCell>
        <TableCell><Mark on={column.nullable === true} /></TableCell>
      </TableRow>)}</TableBody>
    </Table>
  </div>
}
