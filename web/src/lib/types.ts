export type Me = {
  user_id: string
  is_root: boolean
  setup_required: boolean
}

export type Dataset = {
  id: string
  name: string
  description: string
  tier: "hot" | "cold"
  file_count: number
  bytes: number
  cold_file_count: number
  cold_bytes: number
  updated_at: number
}

export type DatasetManifestFile = {
  path: string
  size: number
  sha256: string
  updated_at: number
  tier: "hot" | "cold"
}

export type DatasetManifest = {
  dataset_id: string
  generated_at: number
  files: DatasetManifestFile[]
}

export type Syncer = {
  id: string
  owner_id: string
  name: string
  dataset_id: string
  subdir: string
  created_at: number
  last_synced_at: number | null
}

export type LinkitSettings = {
  owner_id: string
  recipient_username: string
  configured: boolean
  updated_at: number
} | null

export type ArchiveJob = {
  id: string
  dataset_id: string
  status: "running" | "completed" | "failed"
  total_files: number
  archived_files: number
  skipped_files: number
  failed_files: number
  message: string
  created_at: number
  finished_at: number | null
}

export type ColdSettings = {
  bucket: string
  region: string
}

export type SystemResources = {
  sampled_at: number
  cpu: { usage_percent: number; load_1m: number; logical_cpus: number }
  memory: { used_bytes: number; total_bytes: number; available_bytes: number }
  disk: { mount_point: string; used_bytes: number; total_bytes: number; available_bytes: number } | null
  sqlite: { main_bytes: number; wal_bytes: number; shm_bytes: number; total_bytes: number }
}
