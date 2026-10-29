import type { Copy } from "./i18n"
import type { ColumnType, Granularity, TableSpec } from "./types"

// One row of the editor: the declared column plus the three facts that give it
// meaning — is it part of what "the same row" means, is it the time axis, and
// may a writer omit it.
export type ColumnDraft = {
  name: string
  type: ColumnType
  primaryKey: boolean
  timeColumn: boolean
  nullable: boolean
}

export const COLUMN_TYPES: ColumnType[] = ["text", "timestamp", "integer", "real", "boolean"]

const NAME_PATTERN = /^[a-z][a-z0-9_]*$/

export const blankColumn = (): ColumnDraft => ({ name: "", type: "text", primaryKey: false, timeColumn: false, nullable: false })

// A valid starting point: the shape of a tick table, which is what most
// datasets are. Every rule below is already satisfied, so "create" is reachable.
export const starterColumns = (): ColumnDraft[] => [
  { ...blankColumn(), name: "symbol", primaryKey: true },
  { ...blankColumn(), name: "updated_at", type: "timestamp", primaryKey: true, timeColumn: true },
]

// The rules mirror schema::validate exactly, so anything the editor accepts is
// a table the server accepts. Returns the first problem, or null.
export function schemaProblem(columns: ColumnDraft[], t: Copy): string | null {
  if (columns.length === 0) return t.schemaNeedsColumns
  if (columns.some((column) => column.name.trim() === "")) return t.schemaNeedsColumnName
  if (columns.some((column) => !NAME_PATTERN.test(column.name.trim()))) return t.schemaInvalidColumnName
  if (new Set(columns.map((column) => column.name.trim())).size !== columns.length) return t.schemaDuplicateColumn
  if (!columns.some((column) => column.timeColumn)) return t.schemaNeedsTimeColumn
  return null
}

export function buildTableSpec(columns: ColumnDraft[], granularity: Granularity): TableSpec {
  return {
    granularity,
    primary_key: columns.filter((column) => column.primaryKey).map((column) => column.name.trim()),
    time_column: columns.find((column) => column.timeColumn)?.name.trim() ?? "",
    columns: columns.map((column) => ({ name: column.name.trim(), type: column.type, nullable: column.nullable })),
  }
}
