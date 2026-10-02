/**
 * Load the exported gold snapshot and shape it for the page.
 *
 * Same rules as dashboard/data.py: every percentage is recomputed from counts
 * (on_time / departures), never by averaging percentages, so a route with 3
 * departures cannot outweigh one with 3,000.
 */
import Papa from "papaparse";

export const LOCAL_TZ = "America/New_York";
export const DEFAULT_RUSH = [6, 7, 8, 16, 17, 18];

export interface Manifest {
  generated_at_utc?: string;
  service_dates?: { first_service_date: string; last_service_date: string } | null;
  definitions?: { early_s?: number; late_s?: number; rush_hours?: number[] };
  metrics?: {
    avg_events_per_full_day?: number | null;
    total_events?: number;
    days_logged?: number;
    freshness?: { median_s?: number | null; builds?: number };
    rows_removed?: Record<string, { pct_removed?: number | string | null }>;
    data_quality?: { pct_checks_passed?: number | string | null; passed?: number; checks?: number; runs?: number };
    job_runs?: { pct_succeeded?: number | string | null; succeeded?: number; runs?: number };
  } | null;
}

export interface RouteHour {
  route_id: string;
  day_type: "weekday" | "weekend";
  hour_local: number;
  departures: number;
  on_time: number;
  late: number;
  early: number;
  delay_s_sum: number;
  headways: number;
  bunched: number;
  gapped: number;
}

export interface RouteSummary {
  route_id: string;
  route_short_name: string | null;
  route_long_name: string | null;
  departures: number;
  on_time: number;
  late: number;
  avg_delay_s: number;
  p90_delay_s: number;
  rush_departures: number;
  rush_on_time: number;
  headways: number;
  bunched: number;
}

export interface Stop {
  stop_id: string;
  stop_name: string | null;
  lat: number | null;
  lon: number | null;
  departures: number;
  on_time: number;
  avg_delay_s: number;
  route_ids: string | null;
  headways: number;
  bunched: number;
}

export interface WeatherRow {
  weather: "wet" | "dry";
  rush: boolean | string;
  hours: number;
  departures: number;
  on_time: number;
  avg_delay_s: number;
}

export interface ExampleRow {
  service_date: string;
  route_id: string;
  stop_id: string;
  stop_name: string | null;
  trip_id: string;
  vehicle_id: string;
  scheduled_epoch: number;
  observed_epoch: number;
  delay_s: number;
  headway_status: string | null;
}

export interface RunRow {
  run_id: string;
  step: string;
  status: string;
  started_epoch: number;
  duration_s: number;
  bronze_rows: number;
  freshness_s: number | null;
}

export interface Snapshot {
  manifest: Manifest;
  routeHour: RouteHour[];
  routeSummary: RouteSummary[];
  stops: Stop[];
  weather: WeatherRow[];
  example: ExampleRow[];
  runs: RunRow[];
}

/** Text columns that look numeric (route "070", stop ids) must stay text. */
const TEXT_COLUMNS = new Set(["route_id", "route_short_name", "stop_id", "trip_id", "vehicle_id", "run_id"]);

export function parseCsv<T>(text: string): T[] {
  const out = Papa.parse<Record<string, unknown>>(text.trim(), {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: (column) => !TEXT_COLUMNS.has(String(column)),
  });
  return out.data.map((row) => {
    for (const k of Object.keys(row)) if (row[k] === "") row[k] = null;
    return row as T;
  });
}

async function fetchCsv<T>(base: string, name: string): Promise<T[]> {
  const res = await fetch(`${base}/${name}.csv`);
  if (!res.ok) return [];
  return parseCsv<T>(await res.text());
}

/** Load every file; null when there is no manifest (no snapshot exported yet). */
export async function loadSnapshot(base = "/data"): Promise<Snapshot | null> {
  const res = await fetch(`${base}/manifest.json`);
  if (!res.ok || !(res.headers.get("content-type") ?? "").includes("json")) return null;
  const manifest = (await res.json()) as Manifest;
  const [routeHour, routeSummary, stops, weather, example, runs] = await Promise.all([
    fetchCsv<RouteHour>(base, "route_hour"),
    fetchCsv<RouteSummary>(base, "route_summary"),
    fetchCsv<Stop>(base, "stops"),
    fetchCsv<WeatherRow>(base, "weather"),
    fetchCsv<ExampleRow>(base, "bunching_example"),
    fetchCsv<RunRow>(base, "pipeline_runs"),
  ]);
  return { manifest, routeHour, routeSummary, stops, weather, example, runs };
}

export function pct(part: number, whole: number): number | null {
  return whole ? Math.round((1000 * part) / whole) / 10 : null;
}

export function num(v: number | string | null | undefined): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

const sum = <T,>(rows: T[], f: (r: T) => number) => rows.reduce((a, r) => a + (f(r) || 0), 0);

export function isRush(r: Pick<RouteHour, "day_type" | "hour_local">, rush: number[]): boolean {
  return r.day_type === "weekday" && rush.includes(r.hour_local);
}

export interface Kpis {
  departures: number;
  pctOnTime: number | null;
  avgDelayMin: number | null;
  headways: number;
  pctBunched: number | null;
  rushPctOnTime: number | null;
  restPctOnTime: number | null;
}

export function kpis(rows: RouteHour[], rush: number[]): Kpis | null {
  if (!rows.length) return null;
  const r = rows.filter((x) => isRush(x, rush));
  const o = rows.filter((x) => !isRush(x, rush));
  const dep = sum(rows, (x) => x.departures);
  const hw = sum(rows, (x) => x.headways);
  return {
    departures: dep,
    pctOnTime: pct(sum(rows, (x) => x.on_time), dep),
    avgDelayMin: dep ? Math.round(sum(rows, (x) => x.delay_s_sum) / dep / 6) / 10 : null,
    headways: hw,
    pctBunched: pct(sum(rows, (x) => x.bunched), hw),
    rushPctOnTime: pct(sum(r, (x) => x.on_time), sum(r, (x) => x.departures)),
    restPctOnTime: pct(sum(o, (x) => x.on_time), sum(o, (x) => x.departures)),
  };
}

export interface HourPoint {
  hour: number;
  departures: number;
  headways: number;
  pctOnTime: number | null;
  pctBunched: number | null;
}

/** On-time % and bunched % by scheduled hour for one day type, for one route or all routes. */
export function hourProfile(rows: RouteHour[], dayType: string, routeId?: string): HourPoint[] {
  const by = new Map<number, RouteHour[]>();
  for (const r of rows) {
    if (r.day_type !== dayType || (routeId !== undefined && r.route_id !== routeId)) continue;
    by.set(r.hour_local, [...(by.get(r.hour_local) ?? []), r]);
  }
  return [...by.entries()]
    .sort(([a], [b]) => a - b)
    .map(([hour, rs]) => {
      const dep = sum(rs, (x) => x.departures);
      const hw = sum(rs, (x) => x.headways);
      return { hour, departures: dep, headways: hw, pctOnTime: pct(sum(rs, (x) => x.on_time), dep),
               pctBunched: pct(sum(rs, (x) => x.bunched), hw) };
    });
}

export interface RouteRow extends RouteSummary {
  label: string;
  pctOnTime: number;
  rushPctOnTime: number | null;
  pctBunched: number | null;
}

/** Routes with at least `minDepartures` graded departures, least reliable first. */
export function routeTable(rows: RouteSummary[], minDepartures: number): RouteRow[] {
  return rows
    .filter((r) => r.departures >= minDepartures)
    .map((r) => ({
      ...r,
      label: [r.route_short_name ?? r.route_id, r.route_long_name ?? ""].join(" ").trim(),
      pctOnTime: pct(r.on_time, r.departures) ?? 0,
      rushPctOnTime: pct(r.rush_on_time, r.rush_departures),
      pctBunched: pct(r.bunched, r.headways),
    }))
    .sort((a, b) => a.pctOnTime - b.pctOnTime || b.departures - a.departures);
}

/** The largest threshold (from a few round numbers) that still leaves at least `want` routes. */
export function pickMinDepartures(rows: RouteSummary[], want = 10): number {
  for (const t of [100, 50, 30, 20, 10]) if (rows.filter((r) => r.departures >= t).length >= want) return t;
  return 5;
}

export interface WeatherBar {
  period: string;
  dry: number | null;
  wet: number | null;
  dryHours: number;
  wetHours: number;
  dryDepartures: number;
  wetDepartures: number;
}

export function weatherBars(rows: WeatherRow[]): WeatherBar[] {
  const isTrue = (v: boolean | string) => v === true || String(v).toLowerCase() === "true";
  return [true, false].map((rush) => {
    const pick = (w: string) => rows.filter((r) => isTrue(r.rush) === rush && r.weather === w);
    const dry = pick("dry");
    const wet = pick("wet");
    return {
      period: rush ? "Weekday rush" : "Other hours",
      dry: pct(sum(dry, (x) => x.on_time), sum(dry, (x) => x.departures)),
      wet: pct(sum(wet, (x) => x.on_time), sum(wet, (x) => x.departures)),
      dryHours: sum(dry, (x) => x.hours),
      wetHours: sum(wet, (x) => x.hours),
      dryDepartures: sum(dry, (x) => x.departures),
      wetDepartures: sum(wet, (x) => x.departures),
    };
  });
}

/** Bronze rows added per Eastern calendar day, succeeded attempts only. */
export function eventsPerDay(runs: RunRow[]): { date: string; events: number }[] {
  const fmt = new Intl.DateTimeFormat("en-CA", { timeZone: LOCAL_TZ, year: "numeric", month: "2-digit",
                                                 day: "2-digit" });
  const by = new Map<string, number>();
  for (const r of runs) {
    if (r.step !== "bronze" || r.status !== "succeeded") continue;
    const d = fmt.format(new Date(r.started_epoch * 1000));
    by.set(d, (by.get(d) ?? 0) + (r.bronze_rows || 0));
  }
  return [...by.entries()].sort().map(([date, events]) => ({ date, events }));
}

/** "17:05" in Eastern time for an epoch second. */
export function localTime(epoch: number): string {
  return new Intl.DateTimeFormat("en-US", { timeZone: LOCAL_TZ, hour: "2-digit", minute: "2-digit",
                                            hourCycle: "h23" }).format(new Date(epoch * 1000));
}
