# Kod źródłowy aplikacji C4

## Pliki

| Plik | Opis |
|---|---|
| `c4.cpp` | Główny plik aplikacji — subskrypcja MQTT (C2+C3), kolejka FIFO, zapis do InfluxDB |

## Budowanie

```bash
g++ -std=c++17 -O2 c4.cpp -o c4 \
    -lmosquitto -lcurl \
    $(pkg-config --cflags nlohmann_json)
```

Lub przez CMake (jeśli dostępny `CMakeLists.txt`):

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

## Zależności

- `libmosquitto-dev` — klient MQTT (Eclipse Mosquitto C)
- `libcurl4-openssl-dev` — HTTP do InfluxDB API
- `nlohmann/json` — parsowanie JSON (header-only)
