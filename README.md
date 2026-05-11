# C4 — Real-Time Weather & Power Grid Data Collector

Final project for **Real-Time Systems Programming** (AGH EAIiIB AiR KSS, semester 8).
Station C4 continuously collects weather measurements and Polish power grid
(KSE) data from two MQTT sources and stores them in an InfluxDB time-series
database for long-term analysis.

## System architecture (whole group)

```
┌──────────────────────┐     ┌──────────────────────┐
│ C1 — openweathermap  │     │ C3 — pse.pl          │
│ REST API             │     │ web scraping         │
│ weather (PL grid)    │     │ generation, exchange,│
│                      │     │ frequency            │
└──────────┬───────────┘     └──────────┬───────────┘
           │                            │
           ▼                            │
┌──────────────────────┐                │
│ C2                   │                │
│ weather averages     │                │
│ (mutexes)            │                │
└──────────┬───────────┘                │
           │                            │
           │  MQTT  ┌───────────────┐   │  MQTT
           └───────►│   Broker      │◄──┘
                    │  (Mosquitto)  │
                    └───────┬───────┘
                            │ MQTT subscribe
                            ▼
                    ┌───────────────┐
                    │ ► C4 ◄        │   ◄── this repository
                    │ FIFO + write  │
                    └───────┬───────┘
                            │ HTTP API
                            ▼
                    ┌───────────────┐
                    │  InfluxDB 2   │
                    │ (time series) │
                    └───────────────┘
```

The system ran continuously for over 7 days, collecting weather and energy data
into InfluxDB.

## Station C4 responsibilities

Station C4 receives data from two sources and writes it to a networked database:

- **from C2** — raw weather measurements and computed averages
  (temperature, irradiance, wind speed) for Poland,
- **from C3** — Polish National Power Grid (KSE) data scraped from `pse.pl`
  (power demand, generation, cross-border exchange, frequency),
- **FIFO queue** — buffer receiving messages from both sources independently,
- **periodic InfluxDB flush** — timer-driven task that drains the FIFO
  and writes data in batches.

### Threads and synchronization

| Thread | Role | Synchronization |
|---|---|---|
| C2 reader | MQTT subscription for C2 topics | FIFO producer |
| C3 reader | MQTT subscription for C3 topics | FIFO producer |
| DB writer | periodic InfluxDB flush | FIFO consumer, timer |

FIFO access is guarded by a mutex; producer–consumer coordination uses a
condition variable.

## Technology stack

- **Language:** C++ (C++17)
- **MQTT:** Eclipse Mosquitto C client (`libmosquitto`)
- **InfluxDB:** HTTP API via libcurl, Line Protocol format
- **Build:** CMake
- **Infrastructure containerization:** Docker + Docker Compose
- **Target OS:** Linux (tested on Ubuntu)

## Repository structure

```
.
├── src/                    # C4 application source code
├── infra/                  # infrastructure (MQTT broker + database)
│   ├── docker-compose.yml
│   └── mosquitto/config/mosquitto.conf
├── docs/                   # protocol description, MQTT topic diagram, charts
│   ├── wyniki.md           # InfluxDB results from 7-day run
│   └── screenshots/        # PNG captures from Data Explorer
├── .env.example            # environment variable template
├── .gitignore
└── README.md
```

## Local setup

### 1. Start infrastructure (Mosquitto + InfluxDB + MQTT Explorer)

Requires Docker Desktop (Windows/macOS) or Docker Engine (Linux).

```bash
cd infra
cp ../.env.example ../.env       # set your own passwords!
docker compose up -d
```

After startup:

| Service | Address | Notes |
|---|---|---|
| Mosquitto (MQTT) | `tcp://localhost:1883` | no auth (dev mode) |
| InfluxDB UI | http://localhost:8086 | credentials from `.env` |
| MQTT Explorer | http://localhost:4000 | credentials from `.env`, MQTT traffic viewer |

InfluxDB initialises on first start and creates:

- org: `politechnika`
- bucket: `dane_projektowe`

### 2. Build the C4 application

```bash
cd ..
mkdir build && cd build
cmake ..
cmake --build .
```

### 3. Run

```bash
./c4 --mqtt-host=localhost --mqtt-port=1883 \
     --influx-url=http://localhost:8086 \
     --influx-org=politechnika \
     --influx-bucket=dane_projektowe \
     --influx-token=<TOKEN_FROM_INFLUX_UI>
```

Generate the token in the InfluxDB UI (*Load Data → API Tokens*).

## Data format

### MQTT topics (subscribed by C4)

| Topic | Source | Payload |
|---|---|---|
| `projekt/pogoda/C4` | C2 | raw weather measurements + computed averages |
| `projekt/energetyka/C3` | C3 | KSE data (power, generation, exchange, frequency) |

Payload: JSON, UTF-8 encoding.

### InfluxDB schema (Line Protocol)

```
pogoda_pomiary,stanowisko=C2,lat=..,lon=..  temp=...,wind=...,solar=...  <ts>
pogoda_srednie,stanowisko=C2,kraj=Polska    avg_temp=...,avg_wind=...    <ts>
energetyka_podsumowanie,stanowisko=C3       generacja=...,czestotliwosc= <ts>
energetyka_przesyly,kierunek=DE             wartosclac=...,wartosclac_plan=... <ts>
energetyka_status_pse,stanowisko=C3         ...                          <ts>
```

## Build requirements

- C++17 compiler (g++ ≥ 9, clang ≥ 10)
- CMake ≥ 3.16
- libraries: `libmosquitto-dev`, `libcurl4-openssl-dev`, `nlohmann-json3-dev`

Infrastructure:

- Docker ≥ 24
- Docker Compose v2

## Project context (whole group)

| Station | Task | Implemented by |
|---|---|---|
| C1 | weather data from openweathermap.org (REST + timers + semaphores, ≥10 reader tasks) | — |
| C2 | weather averaging across Poland (mutexes) | — |
| C3 | pse.pl web scraping (semaphore + timer) | — |
| **C4** | **receive C2+C3, FIFO, write to networked DB** | **this project** |

General project requirements:

- continuous operation ≥ 7 days,
- standard communication protocol (MQTT — agreed by the group),
- entire implementation in C/C++,
- stations connected via shared network (production: DigitalOcean VPS with
  broker and database; local: this `docker-compose.yml`).

## License

Academic project — for educational purposes.
