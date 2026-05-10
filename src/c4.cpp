#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <curl/curl.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <mosquitto.h>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono_literals;

namespace config {
// vpn 
constexpr const char* MQTT_HOST = "10.255.150.118";
constexpr int         MQTT_PORT = 1883;
constexpr int         MQTT_KEEPALIVE = 60;
constexpr const char* MQTT_CLIENT_ID = "Stanowisko_C4";

// tematy
constexpr const char* TOPIC_C2 = "projekt/pogoda/C4";  // C2 publikuje na ten temat
constexpr const char* TOPIC_C3 = "projekt/energetyka/C3"; // C3 publikuje na ten temat

// InfluxDB lokalnie na vps 
constexpr const char* INFLUX_URL = "http://127.0.0.1:8086/api/v2/write?org=d1088e241a11c9ce&bucket=pomiary&precision=ns";
constexpr const char* INFLUX_TOKEN = "jAF2JbygOV6IIgJGx2n2o_k2R79s6OvfqhMLUBCj4PyrktkEQw4tlOtIdfqN938Gych6VzeqkVAcmQdFouoDAw==";

constexpr std::chrono::seconds DB_FLUSH_PERIOD{10};
constexpr std::size_t MAX_BATCH_SIZE = 500;
}

static std::atomic<bool> g_running{true};

struct RawMessage {
    std::string topic;
    std::string payload;
    std::chrono::system_clock::time_point received_at;
};

struct DbRecord {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::map<std::string, std::string> fields;
    long long timestamp_ns{};
};

template <typename T>
class ThreadSafeQueue {
public:
    void push(T value) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            queue_.push(std::move(value));
        }
        cv_.notify_one();
    }

    bool wait_pop(T& out) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return closed_ || !queue_.empty(); });
        if (queue_.empty()) {
            return false;
        }
        out = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    std::vector<T> drain_all(std::size_t limit = std::numeric_limits<std::size_t>::max()) {
        std::vector<T> out;
        std::lock_guard<std::mutex> lock(mutex_);
        while (!queue_.empty() && out.size() < limit) {
            out.push_back(std::move(queue_.front()));
            queue_.pop();
        }
        return out;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        cv_.notify_all();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<T> queue_;
    bool closed_ = false;
};

struct AppContext {
    ThreadSafeQueue<RawMessage> c2_inbox;
    ThreadSafeQueue<RawMessage> c3_inbox;
    ThreadSafeQueue<DbRecord>   db_fifo;
};

static long long now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

static long long to_ns(const std::chrono::system_clock::time_point& tp) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch()).count();
}

static std::string influx_escape_key(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char c : value) {
        switch (c) {
            case ' ': out += "\\ "; break;
            case ',': out += "\\,"; break;
            case '=': out += "\\="; break;
            default:  out += c; break;
        }
    }
    return out;
}

static std::string influx_escape_tag_value(const std::string& value) {
    return influx_escape_key(value);
}

static std::string influx_escape_string_field(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char c : value) {
        if (c == '"' || c == '\\') {
            out += '\\';
        }
        out += c;
    }
    return out;
}

static std::optional<std::string> json_value_to_influx_field(const json& value) {
    if (value.is_boolean()) {
        return value.get<bool>() ? "true" : "false";
    }
    if (value.is_number_integer()) {
        return std::to_string(value.get<long long>()) + "i";
    }
    if (value.is_number_unsigned()) {
        return std::to_string(value.get<unsigned long long>()) + "i";
    }
    if (value.is_number_float()) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(6) << value.get<double>();
        return oss.str();
    }
    if (value.is_string()) {
        return "\"" + influx_escape_string_field(value.get<std::string>()) + "\"";
    }
    return std::nullopt;
}

static void flatten_json_to_fields(const json& obj, const std::string& prefix, std::map<std::string, std::string>& out) {
    if (!obj.is_object()) {
        return;
    }

    for (auto it = obj.begin(); it != obj.end(); ++it) {
        const std::string key = prefix.empty() ? it.key() : prefix + "_" + it.key();
        if (it->is_object()) {
            flatten_json_to_fields(*it, key, out);
        } else if (it->is_array()) {
            for (std::size_t i = 0; i < it->size(); ++i) {
                if ((*it)[i].is_object()) {
                    flatten_json_to_fields((*it)[i], key + "_" + std::to_string(i), out);
                } else {
                    auto converted = json_value_to_influx_field((*it)[i]);
                    if (converted) {
                        out[key + "_" + std::to_string(i)] = *converted;
                    }
                }
            }
        } else {
            auto converted = json_value_to_influx_field(*it);
            if (converted) {
                out[key] = *converted;
            }
        }
    }
}

static std::string record_to_line_protocol(const DbRecord& record) {
    std::ostringstream line;
    line << influx_escape_key(record.measurement);

    for (const auto& [key, value] : record.tags) {
        line << "," << influx_escape_key(key) << "=" << influx_escape_tag_value(value);
    }

    line << " ";
    bool first = true;
    for (const auto& [key, value] : record.fields) {
        if (!first) {
            line << ",";
        }
        first = false;
        line << influx_escape_key(key) << "=" << value;
    }

    line << " " << record.timestamp_ns;
    return line.str();
}

static bool send_to_influxdb(const std::string& payload) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        std::cerr << "[BLAD] Nie udalo sie zainicjalizowac CURL.\n";
        return false;
    }

    struct curl_slist* headers = nullptr;
    std::string auth = std::string("Authorization: Token ") + config::INFLUX_TOKEN;
    headers = curl_slist_append(headers, auth.c_str());
    headers = curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");

    curl_easy_setopt(curl, CURLOPT_URL, config::INFLUX_URL);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(payload.size()));
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 15L);

    CURLcode res = curl_easy_perform(curl);
    long status = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        std::cerr << "[BLAD] InfluxDB CURL: " << curl_easy_strerror(res) << "\n";
        return false;
    }

    if (status < 200 || status >= 300) {
        std::cerr << "[BLAD] InfluxDB HTTP status: " << status << "\n";
        return false;
    }

    std::cout << "[INFO] Zapisano paczke do InfluxDB.\n";
    return true;
}

static void push_record(AppContext& ctx, DbRecord record) {
    if (record.fields.empty()) {
        std::cerr << "[WARN] Pominieto rekord bez pol.\n";
        return;
    }
    if (record.timestamp_ns == 0) {
        record.timestamp_ns = now_ns();
    }
    ctx.db_fifo.push(std::move(record));
}

// Parser danych z C2.
// C2 wysyla JSON: { "source": "C2", "ts": ...,
//   "averages": { "avg_temp", "avg_wind", "avg_solar", "n_samples" },
//   "raw_data": [ { "lat", "lon", "temp", "wind", "solar", "description", "ts" }, ... ] }
static void process_c2_message(AppContext& ctx, const RawMessage& msg) {
    json j = json::parse(msg.payload);

    long long base_ts = to_ns(msg.received_at);
    if (j.contains("ts") && j["ts"].is_number()) {
        // C2 wysyla ts w sekundach (float) -> nanosekundy
        base_ts = static_cast<long long>(j["ts"].get<double>() * 1'000'000'000.0);
    }

    // 1. Srednie obliczone przez C2 -> measurement "pogoda_srednie"
    if (j.contains("averages") && j["averages"].is_object()) {
        const auto& avg = j["averages"];
        DbRecord rec;
        rec.measurement = "pogoda_srednie";
        rec.tags = {{"stanowisko", "C2"}, {"kraj", "Polska"}};
        rec.timestamp_ns = base_ts;
        flatten_json_to_fields(avg, "", rec.fields);
        push_record(ctx, std::move(rec));
    }

    // 2. Surowe pomiary -> measurement "pogoda_pomiary", osobny rekord per punkt
    if (j.contains("raw_data") && j["raw_data"].is_array()) {
        for (const auto& item : j["raw_data"]) {
            DbRecord rec;
            rec.measurement = "pogoda_pomiary";
            rec.tags = {{"stanowisko", "C2"}};

            // Tworzymy kopie, aby usunac z pol klucze uzyte jako tagi/ts
            json fields_data = item;

            // Wyciagamy reader_id jako tag
            if (item.contains("reader_id") && item["reader_id"].is_number_integer()) {
                rec.tags["reader_id"] = std::to_string(item["reader_id"].get<int>());
                fields_data.erase("reader_id");
            }

            // Uzyj lat/lon jako tagow jesli dostepne
            if (item.contains("lat") && item["lat"].is_number()) {
                std::ostringstream lat_s;
                lat_s << std::fixed << std::setprecision(2) << item["lat"].get<double>();
                rec.tags["lat"] = lat_s.str();
                fields_data.erase("lat");
            }
            if (item.contains("lon") && item["lon"].is_number()) {
                std::ostringstream lon_s;
                lon_s << std::fixed << std::setprecision(2) << item["lon"].get<double>();
                rec.tags["lon"] = lon_s.str();
                fields_data.erase("lon");
            }

            // Timestamp z pomiaru lub bazowy
            if (item.contains("ts") && item["ts"].is_number()) {
                rec.timestamp_ns = static_cast<long long>(item["ts"].get<double>() * 1'000'000'000.0);
                fields_data.erase("ts");
            } else {
                rec.timestamp_ns = base_ts;
            }

            flatten_json_to_fields(fields_data, "", rec.fields);
            push_record(ctx, std::move(rec));
        }
    }

    // 3. Fallback: jesli brak averages i raw_data, zrzuc calosc
    if (!j.contains("averages") && !j.contains("raw_data")) {
        DbRecord rec;
        rec.measurement = "pogoda_pomiary";
        rec.tags = {{"stanowisko", "C2"}};
        rec.timestamp_ns = base_ts;
        flatten_json_to_fields(j, "", rec.fields);
        push_record(ctx, std::move(rec));
    }
}

// parser dla struktury JSON ze stanowiska C3
static void process_c3_message(AppContext& ctx, const RawMessage& msg) {
    json j = json::parse(msg.payload);

    // Uzyj timestampu z JSON (milisekundy -> nanosekundy), lub czasu odebrania
    long long ts_ns;
    if (j.contains("timestamp") && j["timestamp"].is_number()) {
        ts_ns = j["timestamp"].get<long long>() * 1'000'000LL;
    } else {
        ts_ns = to_ns(msg.received_at);
    }

    const auto& data = j.at("data");

    // 1. Podsumowanie energetyki (generacja, zapotrzebowanie, czestotliwosc itp.)
    if (data.contains("podsumowanie") && data["podsumowanie"].is_object()) {
        DbRecord rec;
        rec.measurement = "energetyka_podsumowanie";
        rec.tags = {{"stanowisko", "C3"}, {"kraj", "Polska"}};
        rec.timestamp_ns = ts_ns;
        flatten_json_to_fields(data["podsumowanie"], "", rec.fields);
        push_record(ctx, std::move(rec));
    }

    // 2. Przesyly miedzynarodowe — osobny rekord per kraj, z id jako tagiem
    if (data.contains("przesyly") && data["przesyly"].is_array()) {
        for (const auto& item : data["przesyly"]) {
            DbRecord rec;
            rec.measurement = "energetyka_przesyly";
            rec.tags = {
                {"stanowisko", "C3"},
                {"kraj", "Polska"},
                {"kierunek", item.at("id").get<std::string>()}
            };
            rec.timestamp_ns = ts_ns;

            for (auto it = item.begin(); it != item.end(); ++it) {
                if (it.key() == "id") continue;   // juz uzyty jako tag
                auto converted = json_value_to_influx_field(*it);
                if (converted) {
                    rec.fields[it.key()] = *converted;
                }
            }
            push_record(ctx, std::move(rec));
        }
    }

    // 3. Status PSE w platformach bilansujacych
    if (data.contains("status_pse_w_platformach_bilansujacych") &&
        data["status_pse_w_platformach_bilansujacych"].is_object()) {
        DbRecord rec;
        rec.measurement = "energetyka_status_pse";
        rec.tags = {{"stanowisko", "C3"}, {"kraj", "Polska"}};
        rec.timestamp_ns = ts_ns;
        flatten_json_to_fields(data["status_pse_w_platformach_bilansujacych"], "", rec.fields);
        push_record(ctx, std::move(rec));
    }
}

static void c2_reader_task(AppContext& ctx) {
    RawMessage msg;
    while (g_running || !ctx.c2_inbox.empty()) {
        if (!ctx.c2_inbox.wait_pop(msg)) {
            break;
        }
        try {
            process_c2_message(ctx, msg);
        } catch (const json::parse_error& e) {
            std::cerr << "[BLAD][C2] Nieprawidlowy JSON: " << e.what() << "\n";
        } catch (const std::exception& e) {
            std::cerr << "[BLAD][C2] " << e.what() << "\n";
        }
    }
}

static void c3_reader_task(AppContext& ctx) {
    RawMessage msg;
    while (g_running || !ctx.c3_inbox.empty()) {
        if (!ctx.c3_inbox.wait_pop(msg)) {
            break;
        }
        try {
            process_c3_message(ctx, msg);
        } catch (const json::parse_error& e) {
            std::cerr << "[BLAD][C3] Nieprawidlowy JSON: " << e.what() << "\n";
        } catch (const std::exception& e) {
            std::cerr << "[BLAD][C3] " << e.what() << "\n";
        }
    }
}

static void db_writer_task(AppContext& ctx) {
    while (g_running) {
        std::this_thread::sleep_for(config::DB_FLUSH_PERIOD);

        auto batch = ctx.db_fifo.drain_all(config::MAX_BATCH_SIZE);
        if (batch.empty()) {
            continue;
        }

        std::ostringstream payload;
        for (const auto& record : batch) {
            payload << record_to_line_protocol(record) << "\n";
        }

        if (!send_to_influxdb(payload.str())) {
            std::cerr << "[WARN] Nie udalo sie zapisac paczki. Rekordy zostaly utracone.\n";
        }
    }

    // Ostatnie oproznienie FIFO po zamknieciu aplikacji
    while (!ctx.db_fifo.empty()) {
        auto batch = ctx.db_fifo.drain_all(config::MAX_BATCH_SIZE);
        if (batch.empty()) {
            break;
        }

        std::ostringstream payload;
        for (const auto& record : batch) {
            payload << record_to_line_protocol(record) << "\n";
        }
        send_to_influxdb(payload.str());
    }
}

static void on_connect(struct mosquitto* mosq, void* /*userdata*/, int rc) {
    if (rc != MOSQ_ERR_SUCCESS) {
        std::cerr << "[BLAD] MQTT connect rc=" << rc << "\n";
        return;
    }

    std::cout << "[INFO] Polaczono z brokerem MQTT.\n";

    int sub_rc = mosquitto_subscribe(mosq, nullptr, config::TOPIC_C2, 0);
    if (sub_rc != MOSQ_ERR_SUCCESS) {
        std::cerr << "[BLAD] Subskrypcja C2 nieudana: " << mosquitto_strerror(sub_rc) << "\n";
    }

    sub_rc = mosquitto_subscribe(mosq, nullptr, config::TOPIC_C3, 0);
    if (sub_rc != MOSQ_ERR_SUCCESS) {
        std::cerr << "[BLAD] Subskrypcja C3 nieudana: " << mosquitto_strerror(sub_rc) << "\n";
    }
}

static void on_message(struct mosquitto* /*mosq*/, void* userdata, const struct mosquitto_message* message) {
    if (!userdata || !message || !message->topic || !message->payload) {
        return;
    }

    auto* ctx = static_cast<AppContext*>(userdata);

    RawMessage msg;
    msg.topic = message->topic;
    msg.payload.assign(static_cast<const char*>(message->payload), static_cast<std::size_t>(message->payloadlen));
    msg.received_at = std::chrono::system_clock::now();

    const std::string topic = msg.topic;
    if (topic == config::TOPIC_C2) {
        ctx->c2_inbox.push(std::move(msg));
    } else if (topic == config::TOPIC_C3) {
        ctx->c3_inbox.push(std::move(msg));
    } else {
        std::cerr << "[WARN] Odebrano wiadomosc z nieobslugiwanego tematu: " << topic << "\n";
    }
}

static void handle_signal(int) {
    g_running = false;
}

int main() {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    AppContext ctx;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    mosquitto_lib_init();

    mosquitto* mosq = mosquitto_new(config::MQTT_CLIENT_ID, true, &ctx);
    if (!mosq) {
        std::cerr << "[BLAD] Nie udalo sie utworzyc klienta MQTT.\n";
        curl_global_cleanup();
        mosquitto_lib_cleanup();
        return 1;
    }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_message_callback_set(mosq, on_message);

    std::thread c2_thread(c2_reader_task, std::ref(ctx));
    std::thread c3_thread(c3_reader_task, std::ref(ctx));
    std::thread db_thread(db_writer_task, std::ref(ctx));

    std::cout << "[INFO] Uruchamianie stanowiska C4...\n";
    int rc = mosquitto_connect(mosq, config::MQTT_HOST, config::MQTT_PORT, config::MQTT_KEEPALIVE);
    if (rc != MOSQ_ERR_SUCCESS) {
        std::cerr << "[BLAD] Polaczenie z MQTT nieudane: " << mosquitto_strerror(rc) << "\n";
        g_running = false;
        ctx.c2_inbox.close();
        ctx.c3_inbox.close();
        ctx.db_fifo.close();
        c2_thread.join();
        c3_thread.join();
        db_thread.join();
        mosquitto_destroy(mosq);
        curl_global_cleanup();
        mosquitto_lib_cleanup();
        return 1;
    }

    rc = mosquitto_loop_start(mosq);
    if (rc != MOSQ_ERR_SUCCESS) {
        std::cerr << "[BLAD] Nie udalo sie uruchomic petli MQTT: " << mosquitto_strerror(rc) << "\n";
        g_running = false;
    }

    std::cout << "[INFO] C4 pracuje. Oczekiwanie na dane z C2 i C3...\n";

    while (g_running) {
        std::this_thread::sleep_for(500ms);
    }

    std::cout << "[INFO] Zamykanie aplikacji...\n";
    mosquitto_loop_stop(mosq, true);
    mosquitto_disconnect(mosq);

    ctx.c2_inbox.close();
    ctx.c3_inbox.close();
    ctx.db_fifo.close();

    if (c2_thread.joinable()) c2_thread.join();
    if (c3_thread.joinable()) c3_thread.join();
    if (db_thread.joinable()) db_thread.join();

    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    curl_global_cleanup();

    return 0;
}