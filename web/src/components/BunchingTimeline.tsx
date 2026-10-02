import { useState } from "react";

import { type ExampleRow, localTime } from "../data";

const W = 960;
const H = 190;
const PAD = { left: 92, right: 24, top: 24, bottom: 40 };
const ROW = { scheduled: 62, actual: 122 };

/**
 * One stop, one day: each bus as a dot when it was scheduled (top row) and when it
 * actually left (bottom row), joined by a line. Even spacing on top, clumps below = bunching.
 */
export function BunchingTimeline({ rows }: { rows: ExampleRow[] }) {
  const [hover, setHover] = useState<ExampleRow | null>(null);
  const times = rows.flatMap((r) => [r.scheduled_epoch, r.observed_epoch]);
  const t0 = Math.min(...times);
  const t1 = Math.max(...times);
  const span = Math.max(t1 - t0, 60);
  const x = (t: number) => PAD.left + ((t - t0) / span) * (W - PAD.left - PAD.right);
  const step = span > 3 * 3600 ? 3600 : span > 3600 ? 900 : 300;
  const ticks: number[] = [];
  for (let t = Math.ceil(t0 / step) * step; t <= t1; t += step) ticks.push(t);

  return (
    <div style={{ position: "relative", overflowX: "auto" }}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ minWidth: 640 }} role="img"
           aria-label="Scheduled versus actual departure times of each bus at one stop">
        {ticks.map((t) => (
          <g key={t}>
            <line x1={x(t)} x2={x(t)} y1={PAD.top} y2={H - PAD.bottom} stroke="var(--grid)" />
            <text x={x(t)} y={H - 14} textAnchor="middle" fontSize="12" fill="var(--muted)">{localTime(t)}</text>
          </g>
        ))}
        <text x={0} y={ROW.scheduled + 4} fontSize="13" fill="var(--ink-2)">Scheduled</text>
        <text x={0} y={ROW.actual + 4} fontSize="13" fill="var(--ink-2)">Actual</text>
        {rows.map((r) => (
          <line key={`l${r.trip_id}`} x1={x(r.scheduled_epoch)} y1={ROW.scheduled} x2={x(r.observed_epoch)}
                y2={ROW.actual} stroke="var(--neutral)" strokeOpacity={hover && hover !== r ? 0.25 : 0.7} />
        ))}
        {rows.map((r) => (
          <g key={`d${r.trip_id}`} onMouseEnter={() => setHover(r)} onMouseLeave={() => setHover(null)}
             style={{ cursor: "default" }}>
            <circle cx={x(r.scheduled_epoch)} cy={ROW.scheduled} r={7} fill="var(--neutral)"
                    stroke="var(--surface)" strokeWidth={2} />
            <circle cx={x(r.observed_epoch)} cy={ROW.actual} r={7} fill="var(--series-1)"
                    stroke="var(--surface)" strokeWidth={2} />
            {/* larger invisible hit targets */}
            <circle cx={x(r.scheduled_epoch)} cy={ROW.scheduled} r={14} fill="transparent" />
            <circle cx={x(r.observed_epoch)} cy={ROW.actual} r={14} fill="transparent" />
          </g>
        ))}
      </svg>
      {hover && (
        <div className="tt" style={{ position: "absolute", top: 0, right: 0 }}>
          <div><b>Bus {hover.vehicle_id}</b></div>
          <div><span className="k">Scheduled </span>{localTime(hover.scheduled_epoch)}</div>
          <div><span className="k">Left </span>{localTime(hover.observed_epoch)}
            {" "}({Math.round(hover.delay_s / 60)} min late)</div>
          {hover.headway_status && <div><span className="k">Gap to bus ahead: </span>{hover.headway_status}</div>}
        </div>
      )}
    </div>
  );
}
