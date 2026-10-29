export type Me = {
  user_id: string
  is_root: boolean
  setup_required: boolean
}

export type ColumnType = "integer" | "real" | "text" | "boolean" | "timestamp"

export type CoolingCadence = "daily" | "weekly" | "monthly"

export type Granularity = "hour" | "day" | "month"

export type ColumnSpec = {
  name: string
  type: ColumnType
  nullable?: boolean
}

export type TableSpec = {
  primary_key: string[]
  time_column: string
  granularity: Granularity
  columns: ColumnSpec[]
}

export type TableView = TableSpec & {
  rows: number
  coolable_rows: number
  oldest_time: string | null
  newest_time: string | null
  cold_files: number
  cold_bytes: number
  cold_from: string | null
  cold_until: string | null
}

export type DatasetView = {
  id: string
  name: string
  description: string
  cadence: CoolingCadence
  last_revision: number
  created_at: number
  table: TableView
}

export type ColdFile = {
  dataset_id: string
  revision: number
  path: string
  bucket: string
  rows: number
  bytes: number
  sha256: string
  min_time: string
  max_time: string
  created_at: number
}

export type Manifest = {
  dataset_id: string
  latest_revision: number
  files: ColdFile[]
}

export type Flush = {
  id: string
  dataset_id: string
  pass: number
  trigger: string
  status: "running" | "completed" | "failed"
  partitions: number
  rows: number
  bytes: number
  message: string
  created_at: number
  finished_at: number | null
}

export type CoolingRequest = {
  requested: boolean
  coolable_rows: number
}

export type Syncer = {
  id: string
  owner_id: string
  name: string
  dataset_id: string
  last_revision: number
  created_at: number
  last_synced_at: number | null
}

export type LinkitStatus = {
  configured: boolean
  bot_id: string | null
  recipient_username: string | null
  last_attempt_at: number | null
  last_success_at: number | null
  last_error: string | null
}

export type ColdSettings = {
  bucket: string
  region: string
}

export type WriteToken = {
  id: string
  name: string
  created_at: number
  last_used_at: number | null
}

export type WriteTokenCreated = WriteToken & {
  secret: string
}

export type IngestEvent = {
  id: string
  dataset_id: string
  upserted: number
  deleted: number
  token_id: string
  token_name: string
  created_at: number
}

export type RowsResponse = {
  dataset_id: string
  cadence: CoolingCadence
  rows: Record<string, unknown>[]
}

export type SystemResources = {
  sampled_at: number
  cpu: { usage_percent: number; load_1m: number; logical_cpus: number }
  memory: { used_bytes: number; total_bytes: number; available_bytes: number }
  disk: { mount_point: string; used_bytes: number; total_bytes: number; available_bytes: number } | null
  sqlite: { main_bytes: number; wal_bytes: number; shm_bytes: number; total_bytes: number }
}
