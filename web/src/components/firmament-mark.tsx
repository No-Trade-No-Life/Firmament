export function FirmamentMark({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 32 32" fill="none" className={className} aria-hidden="true">
      <path d="M4 25H28" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" />
      <path d="M6 25a10 10 0 0 1 20 0" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" />
      <circle cx="16" cy="9.5" r="1.7" fill="currentColor" />
      <circle cx="10.5" cy="14.5" r="1.2" fill="currentColor" />
      <circle cx="21.5" cy="14.5" r="1.2" fill="currentColor" />
      <circle cx="14" cy="19.5" r="1" fill="currentColor" />
      <circle cx="18" cy="19.5" r="1" fill="currentColor" />
    </svg>
  )
}
