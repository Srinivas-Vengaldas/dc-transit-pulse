import { useEffect, useMemo, useState } from "react";

import { BunchingTimeline } from "./components/BunchingTimeline";
import { EventsPerDay, HourLines, RainBars, WorstRoutes } from "./components/Charts";
import { StopMap } from "./components/StopMap";
import {
  DEFAULT_RUSH,
  fmtInt,
  headline,
  eventsPerDay,
  hourProfile,
  kpis,
  loadSnapshot,
  num,
  pickMinDepartures,
  routeTable,
  type Snapshot,
  weatherBars,
} from "./data";

const REPO = "https://github.com/Srinivas-Vengaldas/dc-transit-pulse";
const STREAMLIT = import.meta.env.VITE_STREAMLIT_URL as string | undefined;

const fmtPct = (v: number | string | null | undefined) => {
  const n = num(v);
  return n == null ? "n/a" : `${n.toFixed(1)}%`;
};

function Kpi({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="kpi">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      {sub && <div className="sub">{sub}</div>}
    </div>
  );
}

export default function App() {
  const [snap, setSnap] = useState<Snapshot | null | undefined>(undefined);
  useEffect(() => {
    loadSnapshot().then(setSnap).catch(() => setSnap(null));
  }, []);

  if (snap === undefined) return <div className="empty">Loading…</div>;
  if (snap === null) {
    return (
      <div className="wrap empty">
        <h1>DC Transit Pulse</h1>
        <p>No data snapshot yet. Export one with the pipeline's <code>export</code> task and rebuild.</p>
      </div>
    );
  }
  return <Page snap={snap} />;
}

function Page({ snap }: { snap: Snapshot }) {
  const defs = snap.manifest.definitions ?? {};
  const rush = defs.rush_hours ?? DEFAULT_RUSH;
  const k = useMemo(() => kpis(snap.routeHour, rush), [snap, rush]);
  const takeaway = useMemo(() => headline(snap.routeHour, snap.routeSummary, rush), [snap, rush]);
  const minDep = useMemo(() => pickMinDepartures(snap.routeSummary), [snap]);
  const routes = useMemo(() => routeTable(snap.routeSummary, minDep), [snap, minDep]);
  const [routeId, setRouteId] = useState<string | undefined>(undefined);
  const picked = routeId ?? routes[0]?.route_id;
  const m = snap.manifest.metrics ?? {};
  const dates = snap.manifest.service_dates;
  const ex = snap.example[0];
  const lateMin = Math.round((defs.late_s ?? 420) / 60);
  const earlyMin = Math.round((defs.early_s ?? 120) / 60);
  const wetHours = snap.weather.filter((w) => w.weather === "wet").reduce((a, w) => a + w.hours, 0);
  const vp = m.rows_removed?.vehicle_positions;

  return (
    <>
      <header className="hero">
        <div className="wrap">
          <div className="eyebrow">Data engineering portfolio project</div>
          <h1>DC Transit Pulse</h1>
          <p className="lede">
            How reliable are Washington, DC's Metrobuses, and when and where do they bunch? A lakehouse pipeline
            ingests WMATA's live bus feeds every 30 seconds and measures every bus against the timetable in force
            that day.
          </p>
          {takeaway.length > 0 && (
            <div className="takeaway">
              <div className="label">What the data shows</div>
              <p>{takeaway.join(" ")}</p>
            </div>
          )}
          {k && (
            <div className="kpis">
              <Kpi label="Departures on time" value={fmtPct(k.pctOnTime)}
                   sub={`${earlyMin} min early to ${lateMin} min late, at timepoints`} />
              <Kpi label="On time at weekday rush hour" value={fmtPct(k.rushPctOnTime)}
                   sub={k.restPctOnTime == null
                     ? "weekdays 6-9 am and 4-7 pm"
                     : `vs ${fmtPct(k.restPctOnTime)} other hours`} />
              <Kpi label="Gaps where buses bunched" value={fmtPct(k.pctBunched)}
                   sub="next bus under 25% of the scheduled gap" />
              <Kpi label="Average delay" value={k.avgDelayMin == null ? "n/a" : `${k.avgDelayMin > 0 ? "+" : ""}${k.avgDelayMin} min`}
                   sub={`${fmtInt(k.departures)} graded departures`} />
            </div>
          )}
          <p className="note" style={{ marginTop: 16 }}>
            Service dates {dates?.first_service_date ?? "?"} to {dates?.last_service_date ?? "?"}. Snapshot{" "}
            {snap.manifest.generated_at_utc?.replace(/(\d)T(\d)/, "$1 $2").replace("+00:00", " UTC") ?? "?"}. Every number on
            this page is computed from the pipeline's gold tables.
          </p>
          <div className="links">
            <a className="btn primary" href={REPO}>View the code on GitHub</a>
            {STREAMLIT && <a className="btn" href={STREAMLIT}>Open the interactive Streamlit app</a>}
          </div>
        </div>
      </header>

      <nav className="toc">
        <div className="wrap">
          <a href="#problem">The problem</a><a href="#routes">Routes</a><a href="#when">When</a>
          <a href="#where">Where</a><a href="#rain">Rain</a><a href="#pipeline">How it's built</a>
        </div>
      </nav>

      <section id="problem">
        <div className="wrap">
          <h2>Riders plan around the timetable. The buses don't always follow it.</h2>
          <p>
            When a bus runs late it picks up the riders waiting for the next one, so it slows down further while the
            bus behind it catches up. The result is <i>bunching</i>: two or more buses arrive together, then nobody
            comes for twice the scheduled gap. Agencies publish averages; riders feel the gaps.
          </p>
          {ex && (
            <div className="card">
              <div className="chart-title">
                A real example: route {ex.route_id} at {ex.stop_name ?? ex.stop_id}, {ex.service_date}
              </div>
              <div className="chart-sub">
                Top: when the timetable said each bus would leave. Bottom: when it actually did. Lines join the same
                bus. Hover a dot for details.
              </div>
              <div className="legend">
                <span><span className="sw" style={{ background: "var(--neutral)" }} />Scheduled</span>
                <span><span className="sw" style={{ background: "var(--series-1)" }} />Actual</span>
              </div>
              <BunchingTimeline rows={snap.example} />
              <p className="note">
                {snap.example.length} buses observed; {snap.example.filter((r) => r.headway_status === "bunched").length}{" "}
                left bunched with the bus ahead. Picked automatically as the stop-day with the most bunching. Buses are
                observed while the pipeline collects (10 minutes every 2 hours), so this shows the buses seen in those
                windows.
              </p>
            </div>
          )}
        </div>
      </section>

      <section id="routes">
        <div className="wrap">
          <h2>Which routes are least reliable?</h2>
          <p>Share of departures on time, for routes with at least {minDep} graded departures.</p>
          {routes.length ? (
            <div className="card">
              <div className="chart-title">The {Math.min(12, routes.length)} least reliable routes</div>
              <div className="chart-sub">of {routes.length} routes with enough data. Hover for rush-hour and bunching numbers.</div>
              <WorstRoutes rows={routes.slice(0, 12)} />
            </div>
          ) : <p className="note">Not enough data per route yet.</p>}
          {picked && (
            <div className="card">
              <div className="controls">
                <span className="chart-title" style={{ margin: 0 }}>One route through the weekday:</span>
                <select value={picked} onChange={(e) => setRouteId(e.target.value)} aria-label="Route">
                  {routes.map((r) => <option key={r.route_id} value={r.route_id}>{r.label}</option>)}
                </select>
              </div>
              <div className="chart-sub" style={{ marginTop: 8 }}>On-time share by scheduled hour, against all routes.</div>
              <HourLines metric="pctOnTime" domain={[0, 100]} series={[
                { key: "route", name: `Route ${picked}`, color: "var(--series-1)",
                  data: hourProfile(snap.routeHour, "weekday", picked) },
                { key: "all", name: "All routes", color: "var(--neutral)", data: hourProfile(snap.routeHour, "weekday") },
              ]} />
            </div>
          )}
        </div>
      </section>

      <section id="when">
        <div className="wrap">
          <h2>When does service break down?</h2>
          <p>By scheduled hour, weekdays against weekends. Hours with fewer than 20 graded departures are left out.</p>
          <div className="grid-2">
            {(["pctOnTime", "pctBunched"] as const).map((metric) => (
              <div className="card" key={metric}>
                <div className="chart-title">{metric === "pctOnTime" ? "Departures on time" : "Gaps where buses bunched"}</div>
                <div className="chart-sub">{metric === "pctOnTime" ? "Higher is better" : "Lower is better"}</div>
                <HourLines metric={metric} domain={metric === "pctOnTime" ? [0, 100] : undefined} series={
                  (["weekday", "weekend"] as const).map((d, i) => ({
                    key: d, name: d === "weekday" ? "Weekdays" : "Weekends",
                    color: i === 0 ? "var(--series-1)" : "var(--series-2)",
                    data: hourProfile(snap.routeHour, d).filter((p) => p.departures >= 20),
                  }))} />
              </div>
            ))}
          </div>
        </div>
      </section>

      <section id="where">
        <div className="wrap">
          <h2>Where are buses least reliable?</h2>
          <p>
            Each dot is a timepoint stop, sized by departures graded there. <b style={{ color: "var(--good)" }}>Blue</b>{" "}
            stops beat the system average ({fmtPct(k?.pctOnTime)} on time); <b style={{ color: "var(--bad)" }}>red</b>{" "}
            stops fall below it. Hover a dot for details.
          </p>
          <div className="card"><StopMap stops={snap.stops} avg={k?.pctOnTime ?? 50} minDepartures={5} /></div>
        </div>
      </section>

      <section id="rain">
        <div className="wrap">
          <h2>Do delays rise with rain?</h2>
          <p>
            Each route-hour is joined to that hour's weather in downtown DC (Open-Meteo). This snapshot has {wetHours}{" "}
            wet hours, so treat a difference as a pattern to watch until there are more.
          </p>
          <div className="card"><RainBars rows={weatherBars(snap.weather)} /></div>
        </div>
      </section>

      <section id="pipeline">
        <div className="wrap">
          <h2>How it's built</h2>
          <p>
            One Databricks job (serverless, Free Edition) runs every 2 hours from 6 a.m. to 10 p.m. Eastern. Every step
            is idempotent: re-running it adds nothing. This page is static: it reads a snapshot the job publishes only
            after its data-quality checks pass, so it costs nothing to host and holds no credentials.
          </p>
          <div className="card">
            <div className="flow">
              {[
                ["WMATA GTFS-RT", "bus positions + trip updates, protobuf"],
                ["collect", "20 polls, 30 s apart, to a Volume"],
                ["bronze", "Auto Loader, raw + lineage"],
                ["silver", "typed, deduped with watermark + MERGE"],
                ["gold", "departures, on-time, headways"],
                ["quality", "12 SQL checks"],
                ["export", "CSV snapshot"],
              ].map(([t, d]) => (
                <div className="step" key={t}><b>{t}</b><span>{d}</span></div>
              ))}
            </div>
            <p className="note" style={{ marginBottom: 0 }}>
              Also: GTFS static schedule as SCD Type 2 dimensions (point-in-time joins), hourly weather from Open-Meteo
              as an independent task, and pytest + GitHub Actions on every change.
            </p>
          </div>

          <div className="kpis">
            <Kpi label="Events per day" value={m.avg_events_per_full_day ? fmtInt(m.avg_events_per_full_day) : "n/a"}
                 sub={`${m.days_logged ?? 0} day(s) logged`} />
            <Kpi label="Median freshness" value={num(m.freshness?.median_s) ? `${(num(m.freshness?.median_s)! / 60).toFixed(1)} min` : "n/a"}
                 sub="newest departure to gold build" />
            <Kpi label="Positions removed" value={fmtPct(vp?.pct_removed)} sub="duplicates and late rows" />
            <Kpi label="Quality checks passed" value={fmtPct(m.data_quality?.pct_checks_passed)}
                 sub={`${m.data_quality?.passed ?? 0} of ${m.data_quality?.checks ?? 0} over ${m.data_quality?.runs ?? 0} runs`} />
          </div>

          {snap.runs.length > 0 && (
            <div className="card">
              <div className="chart-title">Events ingested per day</div>
              <div className="chart-sub">Rows added to bronze by the scheduled job (both feeds)</div>
              <EventsPerDay rows={eventsPerDay(snap.runs)} />
            </div>
          )}

          <div className="decisions">
            <div><b>Idempotent at every layer</b>Files named by feed timestamp, Auto Loader checkpoints, MERGE on keys,
              replaceWhere by date. A rerun adds 0 rows (measured).</div>
            <div><b>Late and duplicate data</b>30-minute event-time watermark plus an insert-only MERGE on
              (vehicle, timestamp).</div>
            <div><b>Schedule changes</b>Routes and stops as SCD Type 2, so every bus is graded against the timetable in
              force on its service date.</div>
            <div><b>Validated metric</b>Delays derived from GPS pings are cross-checked against WMATA's own
              trip-update delays for the same trips (benchmarks/crosscheck.sql).</div>
          </div>

          <div className="chips">
            {["Python", "PySpark", "Databricks", "Delta Lake", "Auto Loader", "Unity Catalog", "Databricks Workflows",
              "GTFS-Realtime", "SQL", "pytest", "GitHub Actions", "React", "Streamlit"].map((t) => (
              <span className="chip" key={t}>{t}</span>
            ))}
          </div>
        </div>
      </section>

      <footer>
        <div className="wrap">
          <dl>
            <dt>Data</dt><dd>WMATA Bus GTFS-Realtime and GTFS static (developer.wmata.com); weather by Open-Meteo.</dd>
            <dt>On time</dt><dd>Left a timepoint between {earlyMin} minutes early and {lateMin} minutes late.</dd>
            <dt>Departure</dt><dd>Midpoint of the last GPS ping at or before the stop and the first past it, graded only
              when they are at most 2 minutes apart.</dd>
            <dt>Bunched</dt><dd>Gap to the previous bus under 25% of the scheduled gap between the same two trips.</dd>
          </dl>
          <p>Built by Srinivas Vengaldas · <a href={REPO}>Source on GitHub</a></p>
        </div>
      </footer>
    </>
  );
}
