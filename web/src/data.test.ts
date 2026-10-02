import { describe, expect, it } from "vitest";

import {
  eventsPerDay,
  fmtInt,
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
