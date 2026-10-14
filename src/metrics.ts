const counters = new Map<string, number>([['wager_reconciliation_mismatches_total{}', 0]]);
const gauges = new Map<string, number>();
export function count(name: string, labels: Record<string, string> = {}): void {
  const key = name + JSON.stringify(labels);
  counters.set(key, (counters.get(key) ?? 0) + 1);
}
export function gauge(name: string, value: number, labels: Record<string, string> = {}): void { gauges.set(name + JSON.stringify(labels), value); }
export function observe(name: string, value: number): void {
  count(`${name}_count`);
  gauges.set(`${name}_sum{}`, (gauges.get(`${name}_sum{}`) ?? 0) + value);
  for (const boundary of [5, 10, 25, 50, 100, 250, 500, 1000, 5000])
    if (value <= boundary) count(`${name}_bucket`, { le: String(boundary) });
  count(`${name}_bucket`, { le: '+Inf' });
}
export function metrics(): string {
  return [...counters, ...gauges].map(([key, value]) => {
    const position = key.indexOf('{');
    const labels = JSON.parse(key.slice(position)) as Record<string, string>;
    const formatted = Object.entries(labels).map(([name, content]) => `${name}=${JSON.stringify(content)}`).join(',');
    return `${key.slice(0, position)}${formatted ? `{${formatted}}` : ''} ${value}`;
  }).join('\n') + '\n';
}
