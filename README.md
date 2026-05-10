# PSCR — Projekt 2 — Stanowisko C4

Projekt zaliczeniowy z przedmiotu **Programowanie Systemów Czasu Rzeczywistego**
(AIR KSS, semestr 8). Aplikacja realizuje **stanowisko C4** w czteroosobowym
systemie do długoterminowego logowania danych pogodowych oraz danych o pracy
polskiej sieci energetycznej.

## Architektura systemu (cała grupa)

```
┌──────────────────────┐     ┌──────────────────────┐
│ C1 — openweathermap  │     │ C3 — pse.pl          │
│ REST API             │     │ web scraping         │
│ pogoda (siatka PL)   │     │ generacja, wymiana,  │
│                      │     │ częstotliwość        │
└──────────┬───────────┘     └──────────┬───────────┘
           │                            │
           ▼                            │
┌──────────────────────┐                │
│ C2                   │                │
│ średnie pogody       │                │
│ (mutex'y)            │                │
└──────────┬───────────┘                │
           │                            │
           │  MQTT  ┌───────────────┐   │  MQTT
           └───────►│   Broker      │◄──┘
                    │  (Mosquitto)  │
                    └───────┬───────┘
                            │ MQTT subscribe
                            ▼
                    ┌───────────────┐
                    │ ► C4 ◄        │   ◄── to repozytorium
                    │ FIFO + zapis  │
                    └───────┬───────┘
                            │ HTTP API
                            ▼
                    ┌───────────────┐
                    │  InfluxDB 2   │
                    │ (time series) │
                    └───────────────┘
```

System pracował ciągle przez ponad 7 dni, gromadząc dane pogodowe i energetyczne
w bazie InfluxDB.

## Zadanie stanowiska C4

Stanowisko C4 odbiera dane z dwóch źródeł i zapisuje je do sieciowej bazy danych:

- **odbiór ze stanowiska C2** — surowe pomiary pogodowe oraz wyliczone średnie
  (temperatura, nasłonecznienie, wiatr) dla całej Polski,
- **odbiór ze stanowiska C3** — dane o pracy KSE pobrane z `pse.pl`
  (zapotrzebowanie na moc, generacja, wymiana z zagranicą, częstotliwość),
- **kolejka FIFO** — bufor odbierający wiadomości z obu źródeł niezależnie,
- **okresowy zapis do InfluxDB** — zadanie wyzwalane zegarem, opróżnia FIFO
  i zapisuje wsadowo do bazy.

### Wątki i synchronizacja

| Wątek | Rola | Synchronizacja |
|---|---|---|
| Odbiór C2 | subskrypcja tematów MQTT z C2 | producent FIFO |
| Odbiór C3 | subskrypcja tematów MQTT z C3 | producent FIFO |
| Zapis do bazy | okresowy zrzut danych do InfluxDB | konsument FIFO, zegar |

Dostęp do FIFO chroniony mutexem, koordynacja producent–konsument przez
zmienną warunkową.

## Stos technologiczny

- **Język:** C++ (standard C++17)
- **MQTT:** Eclipse Paho MQTT C++
- **InfluxDB:** komunikacja przez HTTP API (libcurl) z formatem Line Protocol
- **Build:** CMake
- **Konteneryzacja infrastruktury:** Docker + Docker Compose
- **System docelowy:** Linux (testowane na Ubuntu)

## Struktura repozytorium

```
.
├── src/                    # kod źródłowy aplikacji C4
├── infra/                  # infrastruktura (broker MQTT + baza danych)
│   ├── docker-compose.yml
│   └── mosquitto/config/mosquitto.conf
├── docs/                   # opis protokołu, schemat tematów MQTT, wykresy
├── .env.example            # szablon zmiennych środowiskowych
├── .gitignore
└── README.md
```

## Uruchomienie lokalne

### 1. Postaw infrastrukturę (Mosquitto + InfluxDB + MQTT Explorer)

Wymagany Docker Desktop (Windows/macOS) lub Docker Engine (Linux).

```bash
cd infra
cp ../.env.example ../.env       # ustaw własne hasła!
docker compose up -d
```

Po uruchomieniu dostępne są:

| Usługa | Adres | Uwagi |
|---|---|---|
| Mosquitto (MQTT) | `tcp://localhost:1883` | bez uwierzytelniania (tryb dev) |
| InfluxDB UI | http://localhost:8086 | login z `.env` |
| MQTT Explorer | http://localhost:4000 | login z `.env`, podgląd ruchu MQTT |

InfluxDB inicjalizuje się przy pierwszym starcie i tworzy:

- organizację: `politechnika`
- bucket: `dane_projektowe`

### 2. Zbuduj aplikację C4

```bash
cd ..
mkdir build && cd build
cmake ..
cmake --build .
```

### 3. Uruchom

```bash
./c4 --mqtt-host=localhost --mqtt-port=1883 \
     --influx-url=http://localhost:8086 \
     --influx-org=politechnika \
     --influx-bucket=dane_projektowe \
     --influx-token=<TOKEN_Z_INFLUX_UI>
```

Token wygeneruj w UI InfluxDB (zakładka *Load Data → API Tokens*).

## Format danych

### Tematy MQTT (subskrybowane przez C4)

| Temat | Źródło | Payload |
|---|---|---|
| `pogoda/raw/<id_komorki>` | C2 | surowy pomiar dla komórki siatki |
| `pogoda/srednie` | C2 | średnie ogólnopolskie |
| `energetyka/pse` | C3 | dane KSE (moc, generacja, wymiana, f) |

Payload JSON, kodowanie UTF-8.

### Schemat zapisu w InfluxDB (Line Protocol)

```
pogoda_raw,cell=<id>     temp=...,wind=...,sun=...     <ts>
pogoda_srednia           temp=...,wind=...,sun=...     <ts>
energetyka_pse,zrodlo=.. moc=...,wymiana=...,freq=...  <ts>
```

## Wymagania środowiskowe

Do budowy aplikacji:

- kompilator C++17 (g++ ≥ 9, clang ≥ 10)
- CMake ≥ 3.16
- biblioteki: `libpaho-mqttpp3-dev`, `libpaho-mqtt3as`, `libcurl4-openssl-dev`, `nlohmann-json3-dev`

Do uruchomienia infrastruktury:

- Docker ≥ 24
- Docker Compose v2

## Kontekst projektu (cała grupa)

| Stanowisko | Zadanie | Realizujący |
|---|---|---|
| C1 | odczyt pogody z openweathermap.org / open-meteo.com (REST + zegary + semafory, ≥10 zadań odczytu) | — |
| C2 | uśrednianie danych pogodowych dla całej Polski (mutex'y) | — |
| C3 | web scraping pse.pl (semafor + zegar) | — |
| **C4** | **odbiór C2+C3, FIFO, zapis do bazy sieciowej** | **ten projekt** |

Wymagania ogólne projektu:

- praca ciągła ≥ 7 dni,
- protokół komunikacji: standard (MQTT — wybrany w grupie),
- całość w C/C++,
- łączność stanowisk przez wspólną sieć (na produkcji: VPS DigitalOcean
  z brokerem i bazą; lokalnie: ten `docker-compose.yml`).

## Licencja

Projekt akademicki — do celów dydaktycznych.
