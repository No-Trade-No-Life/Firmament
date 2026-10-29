import { ChevronLeftIcon, ChevronRightIcon } from "lucide-react"

import { Button } from "@/components/ui/button"

// Structural shape of the pagination slice: any table registered with
// `rowPaginationFeature` satisfies it, so the pager stays free of generics.
export type PagedTable = {
  state: { pagination: { pageIndex: number; pageSize: number } }
  getPageCount(): number
  getCanPreviousPage(): boolean
  getCanNextPage(): boolean
  previousPage(): void
  nextPage(): void
}

export function Pager({ table, label }: { table: PagedTable; label: string }) {
  const page = table.state.pagination.pageIndex + 1
  const count = Math.max(table.getPageCount(), 1)
  return <div className="flex items-center justify-between gap-2 pt-2 text-xs text-muted-foreground">
    <span>{label}</span>
    <div className="flex items-center gap-1">
      <span>{page} / {count}</span>
      <Button type="button" size="icon-sm" variant="ghost" aria-label="previous" disabled={!table.getCanPreviousPage()} onClick={() => table.previousPage()}>
        <ChevronLeftIcon />
      </Button>
      <Button type="button" size="icon-sm" variant="ghost" aria-label="next" disabled={!table.getCanNextPage()} onClick={() => table.nextPage()}>
        <ChevronRightIcon />
      </Button>
    </div>
  </div>
}
