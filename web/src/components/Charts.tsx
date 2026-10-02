import {
  Bar,
  BarChart,
  CartesianGrid,
  LabelList,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { HourPoint, RouteRow, WeatherBar } from "../data";

const AXIS = { stroke: "var(--axis)", tick: { fill: "var(--muted)", fontSize: 12 } };

function Tip({ active, payload, label, fmt }: {
  active?: boolean;
  payload?: { name?: string; value?: number | null; color?: string; payload?: Record<string, unknown> }[];
  label?: string | number;
  fmt: (label: string | number | undefined, p: Record<string, unknown>) => string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="tt">
      <div><b>{fmt(label, payload[0].payload ?? {})}</b></div>
      {payload.map((p) => (
        <div key={p.name}>
          <span className="sw" style={{ display: "inline-block", width: 8, height: 8, borderRadius: 4,
                                         background: p.color, marginRight: 6 }} />
          <span className="k">{p.name} </span>{p.value == null ? "n/a" : `${p.value}%`}
        </div>
      ))}
    </div>
  );
}

const hourLabel = (h: number | string | undefined) => `${h}:00`;

/** Least reliable routes, one bar each, on-time share labelled at the bar end. */
export function WorstRoutes({ rows }: { rows: RouteRow[] }) {
  return (
    <ResponsiveContainer width="100%" height={Math.max(220, rows.length * 30 + 40)}>
      <BarChart data={rows} layout="vertical" margin={{ left: 8, right: 36, top: 4, bottom: 4 }} barSize={16}>
        <CartesianGrid horizontal={false} stroke="var(--grid)" />
        <XAxis type="number" domain={[0, 100]} unit="%" {...AXIS} />
        <YAxis type="category" dataKey="label" width={210} interval={0} {...AXIS}
               tick={{ fill: "var(--ink-2)", fontSize: 12 }} />
        <Tooltip cursor={{ fill: "var(--surface-2)" }} content={({ active, payload }) => {
          const r = payload?.[0]?.payload as RouteRow | undefined;
          if (!active || !r) return null;
          return (
            <div className="tt">
              <div><b>{r.label}</b></div>
              <div><span className="k">On time </span>{r.pctOnTime}% of {r.departures.toLocaleString()} departures</div>
              <div><span className="k">At rush hour </span>{r.rushPctOnTime ?? "n/a"}{r.rushPctOnTime != null && "%"}</div>
              <div><span className="k">Bunched gaps </span>{r.pctBunched ?? "n/a"}{r.pctBunched != null && "%"}</div>
              <div><span className="k">Avg delay </span>{(r.avg_delay_s / 60).toFixed(1)} min</div>
            </div>
          );
        }} />
        <Bar dataKey="pctOnTime" name="On time" fill="var(--series-1)" radius={[0, 4, 4, 0]} isAnimationActive={false}>
          <LabelList dataKey="pctOnTime" position="right" fill="var(--ink-2)" fontSize={12}
                     formatter={(v) => `${Math.round(Number(v))}%`} />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}

type Series = { key: string; name: string; color: string; data: HourPoint[] };

/** Several series of one metric by scheduled hour, merged on the hour. */
export function HourLines({ series, metric, unit = "%", domain }: {
  series: Series[];
  metric: "pctOnTime" | "pctBunched";
  unit?: string;
  domain?: [number, number];
}) {
  const hours = [...new Set(series.flatMap((s) => s.data.map((p) => p.hour)))].sort((a, b) => a - b);
  const data = hours.map((hour) => {
    const row: Record<string, number | null> = { hour };
    for (const s of series) row[s.key] = s.data.find((p) => p.hour === hour)?.[metric] ?? null;
    return row;
  });
  return (
    <>
      <div className="legend">
        {series.map((s) => (
          <span key={s.key}><span className="sw" style={{ background: s.color }} />{s.name}</span>
        ))}
      </div>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={data} margin={{ left: 0, right: 12, top: 8, bottom: 4 }}>
          <CartesianGrid vertical={false} stroke="var(--grid)" />
          <XAxis dataKey="hour" tickFormatter={hourLabel} {...AXIS} />
          <YAxis unit={unit} domain={domain ?? [0, "auto"]} width={48} {...AXIS} />
          <Tooltip content={<Tip fmt={(l) => `Scheduled hour ${hourLabel(l)}`} />}
                   cursor={{ stroke: "var(--axis)" }} />
          {series.map((s) => (
            <Line key={s.key} dataKey={s.key} name={s.name} stroke={s.color} strokeWidth={2} connectNulls
                  dot={{ r: 4, fill: s.color, stroke: "var(--surface)", strokeWidth: 2 }} activeDot={{ r: 6 }}
                  isAnimationActive={false} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}

/** On-time share in dry vs wet hours, side by side for rush and other hours. */
export function RainBars({ rows }: { rows: WeatherBar[] }) {
  return (
    <>
      <div className="legend">
        <span><span className="sw" style={{ background: "var(--neutral)" }} />Dry hours</span>
        <span><span className="sw" style={{ background: "var(--series-1)" }} />Wet hours (≥ 0.1 mm)</span>
      </div>
      <ResponsiveContainer width="100%" height={260}>
        <BarChart data={rows} margin={{ left: 0, right: 12, top: 16, bottom: 4 }} barGap={2} barSize={44}>
          <CartesianGrid vertical={false} stroke="var(--grid)" />
          <XAxis dataKey="period" {...AXIS} tick={{ fill: "var(--ink-2)", fontSize: 13 }} />
          <YAxis unit="%" domain={[0, 100]} width={48} {...AXIS} />
          <Tooltip cursor={{ fill: "var(--surface-2)" }} content={({ active, payload }) => {
            const r = payload?.[0]?.payload as WeatherBar | undefined;
            if (!active || !r) return null;
            return (
              <div className="tt">
                <div><b>{r.period}</b></div>
                <div><span className="k">Dry </span>{r.dry ?? "n/a"}{r.dry != null && "%"} on time
                  ({r.dryHours} h, {r.dryDepartures.toLocaleString()} departures)</div>
                <div><span className="k">Wet </span>{r.wet ?? "n/a"}{r.wet != null && "%"} on time
                  ({r.wetHours} h, {r.wetDepartures.toLocaleString()} departures)</div>
              </div>
            );
          }} />
          <Bar dataKey="dry" name="Dry" fill="var(--neutral)" radius={[4, 4, 0, 0]} isAnimationActive={false}>
            <LabelList dataKey="dry" position="top" fill="var(--ink-2)" fontSize={12}
                       formatter={(v) => (v == null ? "" : `${Math.round(Number(v))}%`)} />
          </Bar>
          <Bar dataKey="wet" name="Wet" fill="var(--series-1)" radius={[4, 4, 0, 0]} isAnimationActive={false}>
            <LabelList dataKey="wet" position="top" fill="var(--ink-2)" fontSize={12}
                       formatter={(v) => (v == null ? "" : `${Math.round(Number(v))}%`)} />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </>
  );
}

/** Rows added to bronze per day: the pipeline's volume. */
export function EventsPerDay({ rows }: { rows: { date: string; events: number }[] }) {
  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={rows} margin={{ left: 8, right: 12, top: 8, bottom: 4 }} barSize={28}>
        <CartesianGrid vertical={false} stroke="var(--grid)" />
        <XAxis dataKey="date" tickFormatter={(d: string) => d.slice(5)} {...AXIS} />
        <YAxis tickFormatter={(v: number) => (v >= 1000 ? `${Math.round(v / 1000)}k` : String(v))} width={48}
               {...AXIS} />
        <Tooltip cursor={{ fill: "var(--surface-2)" }} content={({ active, payload }) => {
          const r = payload?.[0]?.payload as { date: string; events: number } | undefined;
          if (!active || !r) return null;
          return <div className="tt"><b>{r.date}</b><div>{r.events.toLocaleString()} events</div></div>;
        }} />
        <Bar dataKey="events" fill="var(--series-1)" radius={[4, 4, 0, 0]} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  );
}
