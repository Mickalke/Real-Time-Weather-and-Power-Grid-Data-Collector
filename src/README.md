# Kod źródłowy aplikacji C4

Wrzuć tutaj źródła swojej aplikacji C++ (`.cpp`, `.hpp`) oraz `CMakeLists.txt`.

Sugerowana struktura:

```
src/
├── CMakeLists.txt
├── main.cpp
├── mqtt/           # subskrybenci C2 i C3
├── fifo/           # kolejka FIFO + synchronizacja
└── influx/         # klient HTTP do InfluxDB Line Protocol
```

Po dodaniu kodu zaktualizuj `README.md` w katalogu głównym o ewentualne
zmiany w sekcji *Uruchomienie* / *Wymagania*.
