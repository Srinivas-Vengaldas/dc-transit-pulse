import { describe, expect, it } from "vitest";

import {
  DEFAULT_RUSH,
  eventsPerDay,
  fmtInt,
  headline,
  hourProfile,
  kpis,
  localTime,
  parseCsv,
  pickMinDepartures,
  routeTable,
  type RouteHour,
  type RouteSummary,
  weatherBars,
} from "./data";

const RUSH = [6, 7, 8, 16, 17, 18];

function rh(route: string, dayType: "weekday" | "weekend", hour: number, dep: number, onTime: number,
            headways = 0, bunched = 0): RouteHour {
  return { route_id: route, day_type: dayType, hour_local: hour, departures: dep, on_time: onTime,
           late: dep - onTime, early: 0, delay_s_sum: 60 * dep, headways, bunched, gapped: 0 };
}

const ROWS = [rh("A", "weekday", 7, 10, 5, 8, 4), rh("A", "weekday", 12, 30, 27, 20, 0),
              rh("B", "weekend", 7, 60, 30, 10, 1)];

describe("kpis", () => {
  it("weights by counts, not by row", () => {
    const k = kpis(ROWS, RUSH)!;
    expect(k.departures).toBe(100);
    expect(k.pctOnTime).toBe(62); // 62 of 100, not the mean of 50, 90 and 50
    expect(k.rushPctOnTime).toBe(50);
    expect(k.restPctOnTime).toBe(63.3);
    expect(k.pctBunched).toBe(13.2);
    expect(k.avgDelayMin).toBe(1);
    expect(kpis([], RUSH)).toBeNull();
  });
});

describe("hourProfile", () => {
  it("sums one day type by hour", () => {
    expect(hourProfile(ROWS, "weekday").map((p) => [p.hour, p.pctOnTime])).toEqual([[7, 50], [12, 90]]);
    expect(hourProfile(ROWS, "weekday", "B")).toEqual([]);
  });
});

describe("routeTable", () => {
  const base = { late: 0, avg_delay_s: 60, p90_delay_s: 300 };
  const rows: RouteSummary[] = [
    { ...base, route_id: "A", route_short_name: "A", route_long_name: "Alpha", departures: 100, on_time: 90,
      rush_departures: 0, rush_on_time: 0, headways: 0, bunched: 0 },
    { ...base, route_id: "B", route_short_name: null, route_long_name: null, departures: 80, on_time: 40,
      rush_departures: 10, rush_on_time: 2, headways: 10, bunched: 3 },
    { ...base, route_id: "C", route_short_name: "C", route_long_name: "Tiny", departures: 3, on_time: 0,
      rush_departures: 0, rush_on_time: 0, headways: 0, bunched: 0 },
  ];
  it("drops small samples and sorts worst first", () => {
    const t = routeTable(rows, 50);
    expect(t.map((r) => r.route_id)).toEqual(["B", "A"]);
    expect(t[0].label).toBe("B");
    expect(t[0].rushPctOnTime).toBe(20);
    expect(t[1].rushPctOnTime).toBeNull();
  });
  it("picks a threshold that leaves enough routes", () => {
    expect(pickMinDepartures(rows, 2)).toBe(50);
    expect(pickMinDepartures(rows, 3)).toBe(5);
  });
});

describe("parseCsv", () => {
  it("keeps ids as text and blanks as null", () => {
    const [r] = parseCsv<{ route_id: string; departures: number; stop_name: string | null }>(
      "route_id,departures,stop_name\n070,12,\n");
    expect(r).toEqual({ route_id: "070", departures: 12, stop_name: null });
  });
});

describe("weatherBars", () => {
  it("splits rush and other hours and reads CSV booleans", () => {
    const bars = weatherBars([
      { weather: "wet", rush: "True", hours: 2, departures: 10, on_time: 4, avg_delay_s: 0 },
      { weather: "dry", rush: false, hours: 5, departures: 20, on_time: 15, avg_delay_s: 0 },
    ]);
    expect(bars[0]).toMatchObject({ period: "Weekday rush", wet: 40, dry: null, wetHours: 2 });
    expect(bars[1]).toMatchObject({ period: "Other hours", dry: 75, wet: null });
  });
});

describe("pipeline helpers", () => {
  it("counts succeeded bronze rows per Eastern day", () => {
    const runs = [
      { run_id: "1", step: "bronze", status: "succeeded", started_epoch: 1790000000, duration_s: 30,
        bronze_rows: 100, freshness_s: null },
      { run_id: "1", step: "bronze", status: "failed", started_epoch: 1790000000, duration_s: 3,
        bronze_rows: 999, freshness_s: null },
    ];
    expect(eventsPerDay(runs)).toEqual([{ date: "2026-09-21", events: 100 }]);
  });
  it("formats Eastern time", () => {
    expect(localTime(1790000000)).toBe("10:13"); // 14:13 UTC = 10:13 EDT
  });
});

describe("fmtInt", () => {
  it("uses US grouping regardless of locale", () => {
    expect(fmtInt(174452)).toBe("174,452");
    expect(fmtInt(1507)).toBe("1,507");
  });
});

describe("headline", () => {
  const hour = (day_type: "weekday" | "weekend", hour_local: number, departures: number, on_time: number) =>
    ({ route_id: "A1", day_type, hour_local, departures, on_time, late: 0, early: 0, delay_s_sum: 0,
       headways: 10, bunched: 1, gapped: 0 }) as RouteHour;
  const route = (route_id: string, departures: number, on_time: number) =>
    ({ route_id, route_short_name: route_id, route_long_name: null, departures, on_time, late: 0,
       avg_delay_s: 0, p90_delay_s: 0, rush_departures: 0, rush_on_time: 0, headways: 0, bunched: 0 }) as RouteSummary;

  it("states on-time, the rush gap, bunching and the worst route from counts", () => {
    const rows = [hour("weekday", 7, 100, 60), hour("weekday", 12, 100, 80)];
    const lines = headline(rows, [route("A1", 120, 60), route("B2", 120, 100)], DEFAULT_RUSH);
    expect(lines[0]).toBe("70% of 200 graded Metrobus departures left on time.");
    expect(lines[1]).toBe("Weekday rush hour is worse: 60% on time, vs 80% at other hours.");
    expect(lines[2]).toMatch(/^10% of gaps/);
    expect(lines[3]).toContain("A1, was on time 50%");
  });

  it("leaves out a comparison it cannot make", () => {
    const lines = headline([hour("weekday", 7, 10, 5)], [], DEFAULT_RUSH);
    expect(lines.join(" ")).not.toContain("rush hour");
    expect(headline([], [], DEFAULT_RUSH)).toEqual([]);
  });
});
