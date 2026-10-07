// 浏览器端的目录授权同步：把 Firmament 数据集镜像到用户选择的本地目录。
// 同步完成后，本地目录是远端数据集的一个子集（只清理曾经同步过、而现在远端已删除的文件）。

export type SyncProgressPhase = "scanning" | "downloading" | "cleaning" | "completed" | "failed"

export type SyncProgress = {
  phase: SyncProgressPhase
  total: number
  completed: number
  current: string
  error?: string
}

export type SyncSummary = {
  downloaded: number
  removed: number
  skipped: number
  bytes: number
}

export type SyncStateFile = {
  synced_at: number
  dataset_id: string
  files: { path: string; sha256: string; size: number }[]
}

type WritableFileStreamLike = {
  write(data: Uint8Array): Promise<void>
  close(): Promise<void>
}

export type DirectoryHandleLike = {
  getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<DirectoryHandleLike>
  getFileHandle(name: string, options?: { create?: boolean }): Promise<FileHandleLike>
  removeEntry(name: string, options?: { recursive?: boolean }): Promise<void>
  queryPermission?(descriptor: { mode: "readwrite" }): Promise<PermissionState>
  requestPermission?(descriptor: { mode: "readwrite" }): Promise<PermissionState>
}

export type FileHandleLike = {
  getFile(): Promise<File>
  createWritable(options?: { keepExistingData?: boolean }): Promise<WritableFileStreamLike>
}

const stateDirectoryName = ".firmament"
const stateFileName = "sync.json"
const handleDatabaseName = "firmament-sync"
const handleStoreName = "directories"

type Picker = (options?: { id?: string; mode?: "readwrite" }) => Promise<DirectoryHandleLike>

export function supportsDirectorySync(): boolean {
  return typeof window !== "undefined" && typeof (window as unknown as { showDirectoryPicker?: unknown }).showDirectoryPicker === "function"
}

export async function pickDirectory(): Promise<DirectoryHandleLike> {
  const picker = (window as unknown as { showDirectoryPicker?: Picker }).showDirectoryPicker
  if (!picker) throw new Error("showDirectoryPicker is unavailable")
  return picker({ id: "firmament-sync", mode: "readwrite" })
}

export async function ensurePermission(handle: DirectoryHandleLike): Promise<boolean> {
  const descriptor = { mode: "readwrite" as const }
  if (handle.queryPermission && (await handle.queryPermission(descriptor)) === "granted") return true
  if (handle.requestPermission && (await handle.requestPermission(descriptor)) === "granted") return true
  return false
}

function openHandleDatabase(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = window.indexedDB.open(handleDatabaseName, 1)
    request.onupgradeneeded = () => { request.result.createObjectStore(handleStoreName) }
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error ?? new Error("IndexedDB open failed"))
  })
}

export async function saveDirectoryHandle(syncerId: string, handle: DirectoryHandleLike): Promise<void> {
  const database = await openHandleDatabase()
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = database.transaction(handleStoreName, "readwrite")
      transaction.objectStore(handleStoreName).put(handle, syncerId)
      transaction.oncomplete = () => resolve()
      transaction.onerror = () => reject(transaction.error ?? new Error("IndexedDB write failed"))
    })
  } finally {
    database.close()
  }
}

export async function loadDirectoryHandle(syncerId: string): Promise<DirectoryHandleLike | null> {
  const database = await openHandleDatabase()
  try {
    return await new Promise<DirectoryHandleLike | null>((resolve, reject) => {
      const transaction = database.transaction(handleStoreName, "readonly")
      const request = transaction.objectStore(handleStoreName).get(syncerId)
      request.onsuccess = () => resolve((request.result as DirectoryHandleLike | undefined) ?? null)
      request.onerror = () => reject(request.error ?? new Error("IndexedDB read failed"))
    })
  } finally {
    database.close()
  }
}

function pathSegments(path: string): string[] {
  return path.split("/").filter((segment) => segment.length > 0 && segment !== ".")
}

async function resolveDirectory(root: DirectoryHandleLike, segments: string[], create: boolean): Promise<DirectoryHandleLike> {
  let directory = root
  for (const segment of segments) {
    directory = await directory.getDirectoryHandle(segment, { create })
  }
  return directory
}

async function resolveParent(root: DirectoryHandleLike, path: string, create: boolean): Promise<{ directory: DirectoryHandleLike; name: string }> {
  const segments = pathSegments(path)
  const name = segments.pop()
  if (!name) throw new Error(`Invalid path: ${path}`)
  const directory = await resolveDirectory(root, segments, create)
  return { directory, name }
}

function isNotFound(error: unknown): boolean {
  return error instanceof DOMException && error.name === "NotFoundError"
}

async function statFile(root: DirectoryHandleLike, path: string): Promise<{ size: number } | null> {
  try {
    const { directory, name } = await resolveParent(root, path, false)
    const handle = await directory.getFileHandle(name)
    const file = await handle.getFile()
    return { size: file.size }
  } catch (error) {
    if (isNotFound(error)) return null
    throw error
  }
}

async function writeFile(root: DirectoryHandleLike, path: string, bytes: Uint8Array): Promise<void> {
  const { directory, name } = await resolveParent(root, path, true)
  const handle = await directory.getFileHandle(name, { create: true })
  const writable = await handle.createWritable({ keepExistingData: false })
  await writable.write(bytes)
  await writable.close()
}

async function removeFile(root: DirectoryHandleLike, path: string): Promise<void> {
  const { directory, name } = await resolveParent(root, path, false)
  await directory.removeEntry(name)
}

async function readState(root: DirectoryHandleLike): Promise<SyncStateFile | null> {
  try {
    const { directory, name } = await resolveParent(root, `${stateDirectoryName}/${stateFileName}`, false)
    const handle = await directory.getFileHandle(name)
    const file = await handle.getFile()
    return JSON.parse(await file.text()) as SyncStateFile
  } catch (error) {
    if (isNotFound(error) || error instanceof SyntaxError) return null
    throw error
  }
}

async function writeState(root: DirectoryHandleLike, state: SyncStateFile): Promise<void> {
  await writeFile(root, `${stateDirectoryName}/${stateFileName}`, new TextEncoder().encode(`${JSON.stringify(state, null, 2)}\n`))
}

export async function sha256Hex(bytes: ArrayBuffer): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytes)
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

export async function runSync(params: {
  directory: DirectoryHandleLike
  datasetId: string
  prefix: string
  files: { path: string; size: number; sha256: string }[]
  download: (path: string) => Promise<ArrayBuffer>
  onProgress: (progress: SyncProgress) => void
}): Promise<SyncSummary> {
  const { directory, datasetId, prefix, files, download, onProgress } = params
  const selected = files.filter((file) => prefix === "" || file.path.startsWith(prefix))
  const previousState = await readState(directory)
  const previous = new Map((previousState?.files ?? []).map((file) => [file.path, file]))

  const planned: typeof selected = []
  let skipped = 0
  onProgress({ phase: "scanning", total: selected.length, completed: 0, current: "" })
  for (let index = 0; index < selected.length; index += 1) {
    const file = selected[index]
    onProgress({ phase: "scanning", total: selected.length, completed: index, current: file.path })
    const existing = await statFile(directory, file.path)
    const known = previous.get(file.path)
    if (existing && known && known.sha256 === file.sha256 && existing.size === file.size) {
      skipped += 1
      continue
    }
    planned.push(file)
  }

  let downloaded = 0
  let bytes = 0
  for (let index = 0; index < planned.length; index += 1) {
    const file = planned[index]
    onProgress({ phase: "downloading", total: planned.length, completed: index, current: file.path })
    const buffer = await download(file.path)
    const digest = await sha256Hex(buffer)
    if (digest !== file.sha256) {
      throw new Error(`Checksum mismatch for ${file.path}`)
    }
    await writeFile(directory, file.path, new Uint8Array(buffer))
    downloaded += 1
    bytes += file.size
  }
  onProgress({ phase: "downloading", total: planned.length, completed: planned.length, current: "" })

  const keep = new Set(selected.map((file) => file.path))
  const removals = [...previous.keys()].filter((path) => !keep.has(path))
  onProgress({ phase: "cleaning", total: removals.length, completed: 0, current: "" })
  let removed = 0
  for (const path of removals) {
    try {
      await removeFile(directory, path)
      removed += 1
    } catch (error) {
      if (isNotFound(error)) continue
      throw error
    }
  }

  await writeState(directory, {
    synced_at: Math.floor(Date.now() / 1000),
    dataset_id: datasetId,
    files: selected.map((file) => ({ path: file.path, sha256: file.sha256, size: file.size })),
  })
  onProgress({ phase: "completed", total: selected.length, completed: selected.length, current: "" })
  return { downloaded, removed, skipped, bytes }
}
