# C4 Application Source Code

## Files

| File | Description |
|---|---|
| `c4.cpp` | Main application — MQTT subscriber (C2+C3), thread-safe FIFO queue, InfluxDB writer |

## Building

```bash
g++ -std=c++17 -O2 c4.cpp -o c4 \
    -lmosquitto -lcurl \
    $(pkg-config --cflags nlohmann_json)
```

Or via CMake (if `CMakeLists.txt` is available):

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

## Dependencies

- `libmosquitto-dev` — MQTT client (Eclipse Mosquitto C)
- `libcurl4-openssl-dev` — HTTP transport for InfluxDB API
- `nlohmann/json` — JSON parsing (header-only)
