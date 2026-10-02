import "leaflet/dist/leaflet.css";

import { CircleMarker, MapContainer, TileLayer, Tooltip } from "react-leaflet";

import { pct, type Stop } from "../data";

const BLUE = [57, 135, 229];
const RED = [227, 73, 72];
const MID = [176, 175, 170];

/** Diverging around the system average: blue better, red worse, gray at the average. */
function color(p: number, avg: number): string {
  const t = Math.max(-1, Math.min(1, (p - avg) / 25));
  const end = t >= 0 ? BLUE : RED;
  const c = MID.map((m, i) => Math.round(m + (end[i] - m) * Math.abs(t)));
  return `rgb(${c.join(",")})`;
}

export function StopMap({ stops, avg, minDepartures }: { stops: Stop[]; avg: number; minDepartures: number }) {
  const pts = stops
    .filter((s) => s.lat != null && s.lon != null && s.departures >= minDepartures)
    .map((s) => ({ ...s, pctOnTime: pct(s.on_time, s.departures) ?? 0 }));
  if (!pts.length) return <p className="note">No stop has enough graded departures yet.</p>;
  const maxDep = Math.max(...pts.map((p) => p.departures));
  const center: [number, number] = [
    pts.reduce((a, p) => a + (p.lat as number), 0) / pts.length,
    pts.reduce((a, p) => a + (p.lon as number), 0) / pts.length,
  ];
  const dark = window.matchMedia?.("(prefers-color-scheme: dark)").matches;
  return (
    <div className="map">
      <MapContainer center={center} zoom={11} scrollWheelZoom={false} style={{ height: "100%" }}>
        <TileLayer
          url={`https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_${dark ? "Dark" : "Light"}_Gray_Base/MapServer/tile/{z}/{y}/{x}`}
          attribution="Tiles &copy; Esri &mdash; Esri, HERE, Garmin, &copy; OpenStreetMap contributors"
          maxZoom={16}
        />
        {pts
          .sort((a, b) => b.departures - a.departures) // small dots drawn last, on top
          .map((p) => (
            <CircleMarker key={p.stop_id} center={[p.lat as number, p.lon as number]}
                          radius={4 + 8 * Math.sqrt(p.departures / maxDep)}
                          pathOptions={{ color: dark ? "#1a1a19" : "#ffffff", weight: 1,
                                         fillColor: color(p.pctOnTime, avg), fillOpacity: 0.9 }}>
              <Tooltip>
                <b>{p.stop_name ?? p.stop_id}</b><br />
                {p.pctOnTime}% on time ({p.departures} departures)<br />
                {p.headways > 0 && <>Bunched: {pct(p.bunched, p.headways)}% of gaps<br /></>}
                Routes: {p.route_ids}
              </Tooltip>
            </CircleMarker>
          ))}
      </MapContainer>
    </div>
  );
}
