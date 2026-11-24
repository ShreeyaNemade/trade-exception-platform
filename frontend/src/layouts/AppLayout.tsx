import { useState } from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar from '../components/Sidebar';
import { useTheme } from '../hooks/useTheme';
import type { NavItem } from '../types/navigation';

export const NAV_ITEMS: NavItem[] = [
  { label: 'Overview Dashboard', path: '/', description: 'Platform-wide operational summary.' },
  { label: 'Trades', path: '/trades', description: 'Monitor trades through the processing pipeline.' },
  { label: 'Exceptions', path: '/exceptions', description: 'Investigate trade exceptions and root causes.' },
  { label: 'Manual Review', path: '/manual-review', description: 'Trades requiring manual review.' },
  { label: 'Work Items', path: '/work-items', description: 'Track remediation activities.' },
  { label: 'Audit Trail', path: '/audit-trail', description: 'Chronological record of platform events.' },
];

export default function AppLayout() {
  const [open, setOpen] = useState(false);
  const { theme, toggle } = useTheme();

  return (
    <div className="flex min-h-screen">
      <Sidebar items={NAV_ITEMS} open={open} onNavigate={() => setOpen(false)} />
      {open && (
        <div className="fixed inset-0 z-20 bg-black/40 lg:hidden" onClick={() => setOpen(false)} aria-hidden="true" />
      )}
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-16 items-center justify-between border-b border-slate-200 bg-white px-4 dark:border-slate-800 dark:bg-slate-900 sm:px-6">
          <button
            type="button"
            className="rounded-md border border-slate-300 px-2 py-1 text-sm dark:border-slate-700 lg:hidden"
            onClick={() => setOpen(true)}
            aria-label="Open navigation"
          >
            Menu
          </button>
          <span className="hidden text-sm text-slate-500 dark:text-slate-400 lg:block">
            Synthetic data only — educational project
          </span>
          <button
            type="button"
            onClick={toggle}
            className="rounded-md border border-slate-300 px-3 py-1 text-sm hover:bg-slate-100 dark:border-slate-700 dark:hover:bg-slate-800"
          >
            {theme === 'dark' ? 'Light mode' : 'Dark mode'}
          </button>
        </header>
        <main className="flex-1 p-4 sm:p-6 lg:p-8">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
