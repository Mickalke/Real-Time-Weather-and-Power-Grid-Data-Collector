# Results — data collected by C4

The screenshots below come from InfluxDB Data Explorer (VPS `10.255.150.118`).
The system ran without interruption for **over 7 days** (visible range: ~26–31 March 2026).

## Weather data — station C2

Data arrives from weather measurements across a grid of stations in Poland.
Measurement `pogoda_srednie` holds values averaged by C2:
`avg_temp`, `avg_wind`, `avg_solar`, `avg_clouds`, `n_samples`.

### All averages — 2 days

![Weather averages — 2 days](screenshots/pogoda_srednie_2dni.png)

Full set of fields visible: temperature, wind, irradiance, cloud cover, and the
sample count used for averaging. All series are continuous — no gaps in C2
reception.

### Averaged temperature — 2 days

![Temperature — 2 days](screenshots/pogoda_srednie_temp_2dni.png)

Field `avg_temp` (°C) for tag `kraj=Polska`. A clear daily temperature cycle is
visible with peaks around noon.

---

## Energy data — station C3

Data scraped by C3 from `pse.pl`. Measurement `energetyka_podsumowanie` groups
KSE grid parameters: power demand, generation by source, and frequency.
Measurement `energetyka_przesyly` records cross-border exchange with a
`kierunek` (direction) tag.

### KSE summary — 7-day overview

![Energy — 7 days](screenshots/energetyka_podsumowanie_7dni.png)

Overview of all series from `energetyka_podsumowanie` for a full week.
A regular daily rhythm of demand and generation is clearly visible.

### Generation by source — 2 days

![Generation by source — 2 days](screenshots/energetyka_zrodla_2dni.png)

Fields: `PV` (photovoltaic), `cieplne` (thermal), `generacja` (total generation),
`inne` (other), `slatowe` (solar total), and `czestotliwosc` (grid frequency, 50 Hz ± deviations).
A sharp PV peak during daylight hours is visible.

### Cross-border transfers — 7 days

![Transfers — directions 7 days](screenshots/energetyka_przesyly_7dni.png)

Measurement `energetyka_przesyly`, filter `kierunek` ∈ {CZ, DE, LT, SE, SK, UA}.
Fields `wartosclac` and `wartosclac_plan` — actual and planned exchange in MW.
DE and CZ are the dominant directions.

### Transfers — actual vs. planned — 2 days

![Transfers — values 2 days](screenshots/energetyka_przesyly_wartosclac_2dni.png)

Zoom into 2 days for field `wartosclac` (realised exchange, MW).
Series for directions `CZ` and `Polska` (net export) — correlation with the
daily demand profile is visible.

---

## Summary

| Measurement | Cadence | Range |
|---|---|---|
| `pogoda_srednie` | continuous, ~every 60 s | 26–31 March 2026 |
| `pogoda_pomiary` | raw per-station measurements | 26–31 March 2026 |
| `energetyka_podsumowanie` | ~every 5 min | 26–31 March 2026 |
| `energetyka_przesyly` | ~every 5 min, per country | 26–31 March 2026 |
| `energetyka_status_pse` | ~every 5 min | 26–31 March 2026 |

No visible data gaps — C4 maintained continuity despite MQTT reconnects and
VPS restarts during the collection period.
