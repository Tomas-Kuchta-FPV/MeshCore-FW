#include "CoreScopeObserver.h"

#if defined(WITH_CORESCOPE_OBSERVER) && defined(ESP32)

#include <WiFi.h>
#include <WebSocketsClient.h>
#include <MQTTPubSubClient.h>
#include <SHA256.h>
#include <new>
#include <time.h>
#include <freertos/FreeRTOS.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>
#include <freertos/task.h>
#include <esp_wifi.h>

#ifndef CORESCOPE_WIFI_SSID
#define CORESCOPE_WIFI_SSID "Guest"
#endif
#ifndef CORESCOPE_WIFI_PASSWORD
#define CORESCOPE_WIFI_PASSWORD ""
#endif
#ifndef CORESCOPE_MQTT3_HOST
#define CORESCOPE_MQTT3_HOST "mqtt1.meshcore.cz"
#endif
#ifndef CORESCOPE_MQTT3_PORT
#define CORESCOPE_MQTT3_PORT 443
#endif
#ifndef CORESCOPE_MQTT3_TOKEN_AUDIENCE
#define CORESCOPE_MQTT3_TOKEN_AUDIENCE "mqtt1.meshcore.cz"
#endif
#ifndef CORESCOPE_MQTT4_HOST
#define CORESCOPE_MQTT4_HOST "mqtt2.meshcore.website"
#endif
#ifndef CORESCOPE_MQTT4_PORT
#define CORESCOPE_MQTT4_PORT 443
#endif
#ifndef CORESCOPE_MQTT4_TOKEN_AUDIENCE
#define CORESCOPE_MQTT4_TOKEN_AUDIENCE "mqtt2.meshcore.website"
#endif
#ifndef CORESCOPE_MQTT_WS_PATH
#define CORESCOPE_MQTT_WS_PATH "/"
#endif
#ifndef CORESCOPE_IATA
#define CORESCOPE_IATA "JCL"
#endif
#ifndef CORESCOPE_OBSERVER_NAME
#define CORESCOPE_OBSERVER_NAME "XIAO MeshCore Repeater"
#endif
#ifndef CORESCOPE_MODEL
#define CORESCOPE_MODEL "MeshCore ESP32 repeater"
#endif
#ifndef CORESCOPE_QUEUE_LENGTH
#define CORESCOPE_QUEUE_LENGTH 12
#endif
#ifndef CORESCOPE_TASK_STACK_SIZE
#define CORESCOPE_TASK_STACK_SIZE 16384
#endif
#ifndef FIRMWARE_VERSION
#define FIRMWARE_VERSION "dev"
#endif

namespace {

constexpr size_t kMaxRawPacket = 255;
constexpr uint32_t kAuthTokenLifetimeSeconds = 86400;

// Both Czech brokers are served through Cloudflare. Trust both root families
// currently used for Cloudflare edge certificates, while still verifying the
// hostname and certificate chain in WiFiClientSecure.
static const char kBrokerRootCAs[] PROGMEM = R"EOF(
-----BEGIN CERTIFICATE-----
MIICCTCCAY6gAwIBAgINAgPlwGjvYxqccpBQUjAKBggqhkjOPQQDAzBHMQswCQYD
VQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2VzIExMQzEUMBIG
A1UEAxMLR1RTIFJvb3QgUjQwHhcNMTYwNjIyMDAwMDAwWhcNMzYwNjIyMDAwMDAw
WjBHMQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2Vz
IExMQzEUMBIGA1UEAxMLR1RTIFJvb3QgUjQwdjAQBgcqhkjOPQIBBgUrgQQAIgNi
AATzdHOnaItgrkO4NcWBMHtLSZ37wWHO5t5GvWvVYRg1rkDdc/eJkTBa6zzuhXyi
QHY7qca4R9gq55KRanPpsXI5nymfopjTX15YhmUPoYRlBtHci8nHc8iMai/lxKvR
HYqjQjBAMA4GA1UdDwEB/wQEAwIBhjAPBgNVHRMBAf8EBTADAQH/MB0GA1UdDgQW
BBSATNbrdP9JNqPV2Py1PsVq8JQdjDAKBggqhkjOPQQDAwNpADBmAjEA6ED/g94D
9J+uHXqnLrmvT/aDHQ4thQEd0dlq7A/Cr8deVl5c1RxYIigL9zC2L7F8AjEA8GE8
p/SgguMh1YQdc4acLa/KNJvxn7kjNuK8YAOdgLOaVsjh4rsUecrNIdSUtUlD
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIFazCCA1OgAwIBAgIRAIIQz7DSQONZRGPgu2OCiwAwDQYJKoZIhvcNAQELBQAw
TzELMAkGA1UEBhMCVVMxKTAnBgNVBAoTIEludGVybmV0IFNlY3VyaXR5IFJlc2Vh
cmNoIEdyb3VwMRUwEwYDVQQDEwxJU1JHIFJvb3QgWDEwHhcNMTUwNjA0MTEwNDM4
WhcNMzUwNjA0MTEwNDM4WjBPMQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJu
ZXQgU2VjdXJpdHkgUmVzZWFyY2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBY
MTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAK3oJHP0FDfzm54rVygc
h77ct984kIxuPOZXoHj3dcKi/vVqbvYATyjb3miGbESTtrFj/RQSa78f0uoxmyF+
0TM8ukj13Xnfs7j/EvEhmkvBioZxaUpmZmyPfjxwv60pIgbz5MDmgK7iS4+3mX6U
A5/TR5d8mUgjU+g4rk8Kb4Mu0UlXjIB0ttov0DiNewNwIRt18jA8+o+u3dpjq+sW
T8KOEUt+zwvo/7V3LvSye0rgTBIlDHCNAymg4VMk7BPZ7hm/ELNKjD+Jo2FR3qyH
B5T0Y3HsLuJvW5iB4YlcNHlsdu87kGJ55tukmi8mxdAQ4Q7e2RCOFvu396j3x+UC
B5iPNgiV5+I3lg02dZ77DnKxHZu8A/lJBdiB3QW0KtZB6awBdpUKD9jf1b0SHzUv
KBds0pjBqAlkd25HN7rOrFleaJ1/ctaJxQZBKT5ZPt0m9STJEadao0xAH0ahmbWn
OlFuhjuefXKnEgV4We0+UXgVCwOPjdAvBbI+e0ocS3MFEvzG6uBQE3xDk3SzynTn
jh8BCNAw1FtxNrQHusEwMFxIt4I7mKZ9YIqioymCzLq9gwQbooMDQaHWBfEbwrbw
qHyGO0aoSCqI3Haadr8faqU9GY/rOPNk3sgrDQoo//fb4hVC1CLQJ13hef4Y53CI
rU7m2Ys6xt0nUW7/vGT1M0NPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNV
HRMBAf8EBTADAQH/MB0GA1UdDgQWBBR5tFnme7bl5AFzgAiIyBpY9umbbjANBgkq
hkiG9w0BAQsFAAOCAgEAVR9YqbyyqFDQDLHYGmkgJykIrGF1XIpu+ILlaS/V9lZL
ubhzEFnTIZd+50xx+7LSYK05qAvqFyFWhfFQDlnrzuBZ6brJFe+GnY+EgPbk6ZGQ
3BebYhtF8GaV0nxvwuo77x/Py9auJ/GpsMiu/X1+mvoiBOv/2X/qkSsisRcOj/KK
NFtY2PwByVS5uCbMiogziUwthDyC3+6WVwW6LLv3xLfHTjuCvjHIInNzktHCgKQ5
ORAzI4JMPJ+GslWYHb4phowim57iaztXOoJwTdwJx4nLCgdNbOhdjsnvzqvHu7Ur
TkXWStAmzOVyyghqpZXjFaH3pO3JLF+l+/+sKAIuvtd7u+Nxe5AW0wdeRlN8NwdC
jNPElpzVmbUq4JUagEiuTDkHzsxHpFKVK7q4+63SM1N95R1NbdWhscdCb+ZAJzVc
oyi3B43njTOQ5yOf+1CceWxG1bQVs5ZufpsMljq4Ui0/1lvh+wjChP4kqKOJ2qxq
4RgqsahDYVvTH9w7jXbyLeiNdd8XM2w9U/t7y0Ff/9yi0GE44Za4rF2LN9d11TPA
mRGunUHBcnWEvgJBQl9nJEiU0Zsnvgc/ubhPgXRR4Xq37Z0j4r7g1SgEEzwxA57d
emyPxgcYxn/eR44/KJ4EBs+lVDR3veyJm+kXQ99b21/+jh5Xos1AnX5iItreGCc=
-----END CERTIFICATE-----
)EOF";

struct RxItem {
  uint8_t len;
  int16_t snr_x10;
  int16_t rssi_x10;
  uint8_t raw[kMaxRawPacket];
};

using MqttClient = MQTTPubSub::PubSubClient<1280>;

struct BrokerState {
  const char *name;
  const char *host;
  uint16_t port;
  const char *audience;
  WebSocketsClient websocket;
  // The default MQTT alias has only a 128-byte buffer. Analyzer packets need
  // roughly 1 kB for a maximum-length MeshCore frame and its metadata.
  MqttClient mqtt;
  QueueHandle_t queue = nullptr;
  bool websocket_started = false;
  bool websocket_was_connected = false;
  uint8_t reconnect_step = 0;
  uint32_t next_backoff_ms = 0;
  uint32_t last_status_ms = 0;
  uint32_t published = 0;
  uint32_t dropped = 0;

  BrokerState(const char *broker_name, const char *broker_host,
              uint16_t broker_port, const char *token_audience)
      : name(broker_name), host(broker_host), port(broker_port),
        audience(token_audience) {}
};

BrokerState broker3{"MQTT1 Czech", CORESCOPE_MQTT3_HOST, CORESCOPE_MQTT3_PORT,
                    CORESCOPE_MQTT3_TOKEN_AUDIENCE};
BrokerState broker4{"MQTT2 Czech", CORESCOPE_MQTT4_HOST, CORESCOPE_MQTT4_PORT,
                    CORESCOPE_MQTT4_TOKEN_AUDIENCE};
BrokerState *brokers[] = {&broker3, &broker4};
SemaphoreHandle_t signing_mutex = nullptr;
TaskHandle_t wifi_task_handle = nullptr;
char observer_id[2 * PUB_KEY_SIZE + 1] = {};
char packet_topic[64 + 2 * PUB_KEY_SIZE] = {};
char status_topic[64 + 2 * PUB_KEY_SIZE] = {};
volatile uint32_t dropped = 0;
const mesh::LocalIdentity *local_identity = nullptr;
const char *wifi_ssid = CORESCOPE_WIFI_SSID;
const char *wifi_password = CORESCOPE_WIFI_PASSWORD;
const char *mqtt_ws_path = CORESCOPE_MQTT_WS_PATH;
const char *observer_name = CORESCOPE_OBSERVER_NAME;
const char *observer_model = CORESCOPE_MODEL;
char radio_description[96] = "";
constexpr uint32_t kReconnectDelaysMs[] = {3000, 6000, 12000, 30000, 60000};
constexpr uint32_t kWifiConnectTimeoutMs = 20000;
constexpr uint32_t kWifiDhcpTimeoutMs = 20000;

enum class WifiPhase : uint8_t {
  CONNECTING,
  WAITING_FOR_IP,
  ONLINE,
  RETRY_WAIT
};

WifiPhase wifi_phase = WifiPhase::CONNECTING;
volatile bool wifi_online = false;
uint8_t wifi_retry_step = 0;
uint32_t wifi_deadline_ms = 0;

bool hasUsableIpAddress() {
  const IPAddress ip = WiFi.localIP();
  return ip[0] != 0 || ip[1] != 0 || ip[2] != 0 || ip[3] != 0;
}

void startWifiAttempt() {
  wifi_online = false;
  wifi_phase = WifiPhase::CONNECTING;
  wifi_deadline_ms = millis() + kWifiConnectTimeoutMs;
  Serial.printf("[CoreScope WiFi] connecting to %s\n", wifi_ssid);
  WiFi.begin(wifi_ssid, wifi_password);
}

void scheduleWifiRetry(const char *reason) {
  wifi_online = false;
  WiFi.disconnect(false, false);
  const uint32_t delay_ms = kReconnectDelaysMs[wifi_retry_step];
  if (wifi_retry_step + 1 <
      sizeof(kReconnectDelaysMs) / sizeof(kReconnectDelaysMs[0])) {
    ++wifi_retry_step;
  }
  wifi_phase = WifiPhase::RETRY_WAIT;
  wifi_deadline_ms = millis() + delay_ms;
  Serial.printf("[CoreScope WiFi] %s; retry in %lu s\n", reason,
                static_cast<unsigned long>(delay_ms / 1000));
}

void updateWifiState() {
  const uint32_t now = millis();
  wifi_ap_record_t ap_info;
  const bool associated = esp_wifi_sta_get_ap_info(&ap_info) == ESP_OK;
  const bool has_ip = associated && hasUsableIpAddress();

  if (has_ip) {
    if (wifi_phase != WifiPhase::ONLINE) {
      const IPAddress ip = WiFi.localIP();
      wifi_phase = WifiPhase::ONLINE;
      wifi_online = true;
      wifi_retry_step = 0;
      Serial.printf("[CoreScope WiFi] online ssid=%s ip=%s rssi=%d dBm\n",
                    wifi_ssid, ip.toString().c_str(), WiFi.RSSI());
    }
    return;
  }

  wifi_online = false;
  if (wifi_phase == WifiPhase::ONLINE) {
    scheduleWifiRetry("connection lost");
    return;
  }

  if (associated && wifi_phase != WifiPhase::WAITING_FOR_IP) {
    wifi_phase = WifiPhase::WAITING_FOR_IP;
    wifi_deadline_ms = now + kWifiDhcpTimeoutMs;
    Serial.println("[CoreScope WiFi] associated; waiting for DHCP address");
    return;
  }

  if (!associated && wifi_phase == WifiPhase::WAITING_FOR_IP) {
    scheduleWifiRetry("connection lost before DHCP completed");
    return;
  }

  if (static_cast<int32_t>(now - wifi_deadline_ms) < 0) return;

  if (wifi_phase == WifiPhase::RETRY_WAIT) {
    startWifiAttempt();
  } else if (wifi_phase == WifiPhase::WAITING_FOR_IP) {
    scheduleWifiRetry("DHCP timeout");
  } else {
    scheduleWifiRetry("connection timeout");
  }
}

void wifiTask(void *) {
  for (;;) {
    updateWifiState();
    vTaskDelay(pdMS_TO_TICKS(250));
  }
}

size_t base64UrlEncode(const uint8_t *src, size_t len, char *dst, size_t capacity) {
  static constexpr char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  const size_t required = (len * 4 + 2) / 3;
  if (capacity <= required) return 0;
  size_t in = 0, out = 0;
  while (in + 3 <= len) {
    const uint32_t value = (static_cast<uint32_t>(src[in]) << 16) |
                           (static_cast<uint32_t>(src[in + 1]) << 8) | src[in + 2];
    dst[out++] = alphabet[(value >> 18) & 0x3f];
    dst[out++] = alphabet[(value >> 12) & 0x3f];
    dst[out++] = alphabet[(value >> 6) & 0x3f];
    dst[out++] = alphabet[value & 0x3f];
    in += 3;
  }
  if (in < len) {
    uint32_t value = static_cast<uint32_t>(src[in]) << 16;
    dst[out++] = alphabet[(value >> 18) & 0x3f];
    if (++in < len) value |= static_cast<uint32_t>(src[in]) << 8;
    dst[out++] = alphabet[(value >> 12) & 0x3f];
    if (in < len) dst[out++] = alphabet[(value >> 6) & 0x3f];
  }
  dst[out] = '\0';
  return out;
}

bool buildAuthToken(const char *audience, char *token, size_t capacity) {
  if (!local_identity) return false;
  const time_t now = time(nullptr);
  if (now < 1700000000) return false;

  static constexpr char header[] = "{\"alg\":\"Ed25519\",\"typ\":\"JWT\"}";
  char payload[240];
  const int payload_len = snprintf(payload, sizeof(payload),
      "{\"publicKey\":\"%s\",\"iat\":%lld,\"exp\":%lld,\"aud\":\"%s\"}",
      observer_id, static_cast<long long>(now),
      static_cast<long long>(now + kAuthTokenLifetimeSeconds),
      audience);
  if (payload_len <= 0 || static_cast<size_t>(payload_len) >= sizeof(payload)) return false;

  char header64[64], payload64[336];
  if (!base64UrlEncode(reinterpret_cast<const uint8_t *>(header), strlen(header),
                       header64, sizeof(header64)) ||
      !base64UrlEncode(reinterpret_cast<const uint8_t *>(payload), payload_len,
                       payload64, sizeof(payload64))) return false;

  char signing_input[420];
  const int signing_len = snprintf(signing_input, sizeof(signing_input), "%s.%s",
                                   header64, payload64);
  if (signing_len <= 0 || static_cast<size_t>(signing_len) >= sizeof(signing_input)) return false;

  uint8_t signature[SIGNATURE_SIZE];
  if (!signing_mutex || xSemaphoreTake(signing_mutex, pdMS_TO_TICKS(5000)) != pdTRUE) return false;
  local_identity->sign(signature,
      reinterpret_cast<const uint8_t *>(signing_input), signing_len);
  xSemaphoreGive(signing_mutex);
  char signature_hex[2 * SIGNATURE_SIZE + 1];
  static constexpr char lower_hex[] = "0123456789abcdef";
  for (size_t i = 0; i < SIGNATURE_SIZE; ++i) {
    signature_hex[2 * i] = lower_hex[signature[i] >> 4];
    signature_hex[2 * i + 1] = lower_hex[signature[i] & 0x0f];
  }
  signature_hex[2 * SIGNATURE_SIZE] = '\0';
  const int token_len = snprintf(token, capacity, "%s.%s", signing_input, signature_hex);
  return token_len > 0 && static_cast<size_t>(token_len) < capacity;
}

void toHex(const uint8_t *src, size_t len, char *dst) {
  static constexpr char hex[] = "0123456789ABCDEF";
  for (size_t i = 0; i < len; ++i) {
    dst[2 * i] = hex[src[i] >> 4];
    dst[2 * i + 1] = hex[src[i] & 0x0f];
  }
  dst[2 * len] = '\0';
}

bool parsePacket(const RxItem &item, uint8_t &packet_type, char &route,
                 uint8_t &path_len_wire, size_t &payload_offset,
                 size_t &payload_len) {
  if (item.len < 2) return false;
  const uint8_t header = item.raw[0];
  const uint8_t route_type = header & 0x03;
  packet_type = (header >> 2) & 0x0f;
  route = route_type == ROUTE_TYPE_DIRECT ? 'D' :
          route_type == ROUTE_TYPE_TRANSPORT_DIRECT ? 'T' : 'F';

  size_t offset = 1 + ((route_type == ROUTE_TYPE_TRANSPORT_FLOOD ||
                        route_type == ROUTE_TYPE_TRANSPORT_DIRECT) ? 4 : 0);
  if (offset >= item.len) return false;
  path_len_wire = item.raw[offset++];
  const size_t hop_count = path_len_wire & 0x3f;
  const size_t bytes_per_hop = (path_len_wire >> 6) + 1;
  size_t path_bytes = hop_count * bytes_per_hop;
  // Hash-size mode 4 is reserved. Match meshcore-packet-capture's legacy fallback.
  if (bytes_per_hop == 4 || path_bytes > MAX_PATH_SIZE) path_bytes = path_len_wire;
  if (offset + path_bytes > item.len) return false;
  payload_offset = offset + path_bytes;
  payload_len = item.len - payload_offset;
  return true;
}

void packetHash(const RxItem &item, uint8_t packet_type, uint8_t path_len_wire,
                size_t payload_offset, size_t payload_len, char out[17]) {
  SHA256 sha;
  uint8_t digest[32];
  sha.update(&packet_type, 1);
  if (packet_type == PAYLOAD_TYPE_TRACE) {
    const uint8_t path_len_le[2] = {path_len_wire, 0};
    sha.update(path_len_le, sizeof(path_len_le));
  }
  sha.update(item.raw + payload_offset, payload_len);
  sha.finalize(digest, sizeof(digest));
  toHex(digest, 8, out);
}

bool utcFields(char timestamp[25], char clock_time[9], char date[11]) {
  const time_t now = time(nullptr);
  if (now < 1700000000) return false;
  struct tm tm_utc;
  gmtime_r(&now, &tm_utc);
  strftime(timestamp, 25, "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
  strftime(clock_time, 9, "%H:%M:%S", &tm_utc);
  strftime(date, 11, "%d/%m/%Y", &tm_utc);
  return true;
}

void buildStatus(char *json, size_t capacity, const char *status) {
  char timestamp[25] = "";
  char clock_time[9], date[11];
  utcFields(timestamp, clock_time, date);
  snprintf(json, capacity,
      "{\"status\":\"%s\",\"timestamp\":\"%s\",\"origin\":\"%s\","
      "\"origin_id\":\"%s\",\"model\":\"%s\","
      "\"firmware_version\":\"%s\",\"client_version\":\"repeater-observer-v1.17\","
      "\"radio\":\"%s\",\"repeat\":true,\"stats\":{\"uptime_secs\":%lu}}",
      status, timestamp, observer_name, observer_id, observer_model,
      FIRMWARE_VERSION, radio_description,
      static_cast<unsigned long>(millis() / 1000UL));
}

void publishStatus(BrokerState &broker) {
  char json[480];
  buildStatus(json, sizeof(json), "online");
  if (broker.mqtt.publish(status_topic, String(json), true, 0)) {
    broker.last_status_ms = millis();
  }
}

void resetMqtt(BrokerState &broker) {
  // MQTTPubSubClient does not clear its internal is_connected flag when the
  // underlying WebSocket disappears. Reconstructing only the MQTT layer makes
  // the next WebSocket session send a fresh MQTT CONNECT and a new JWT.
  broker.mqtt.~MqttClient();
  new (&broker.mqtt) MqttClient();
  broker.mqtt.begin(broker.websocket);
  broker.mqtt.setOptions(120, true, 5000);
}

void updateReconnectState(BrokerState &broker) {
  const bool connected = broker.websocket.isConnected();
  const uint32_t now = millis();

  if (connected) {
    if (!broker.websocket_was_connected) {
      broker.reconnect_step = 0;
      broker.websocket.setReconnectInterval(kReconnectDelaysMs[0]);
      Serial.printf("[%s] WebSocket connected\n", broker.name);
    }
    broker.websocket_was_connected = true;
    return;
  }

  if (broker.websocket_was_connected) {
    broker.websocket_was_connected = false;
    broker.reconnect_step = 0;
    broker.websocket.setReconnectInterval(kReconnectDelaysMs[0]);
    broker.next_backoff_ms = now + kReconnectDelaysMs[0];
    resetMqtt(broker);
    Serial.printf("[%s] disconnected; retry in 3 s\n", broker.name);
    return;
  }

  if (broker.next_backoff_ms == 0) {
    broker.next_backoff_ms = now + kReconnectDelaysMs[0];
    return;
  }
  if (static_cast<int32_t>(now - broker.next_backoff_ms) < 0) return;

  if (broker.reconnect_step + 1 <
      sizeof(kReconnectDelaysMs) / sizeof(kReconnectDelaysMs[0])) {
    ++broker.reconnect_step;
  }
  const uint32_t delay_ms = kReconnectDelaysMs[broker.reconnect_step];
  broker.websocket.setReconnectInterval(delay_ms);
  broker.next_backoff_ms = now + delay_ms;
  Serial.printf("[%s] next retry interval %lu s\n", broker.name,
                static_cast<unsigned long>(delay_ms / 1000));
}

void ensureMqtt(BrokerState &broker) {
  if (!wifi_online) return;
  // TLS and JWT both require a valid wall clock. This wait happens only in the
  // observer task and never stalls MeshCore radio processing.
  if (time(nullptr) < 1700000000) return;
  if (!broker.websocket_started) {
    broker.websocket.beginSslWithCA(broker.host, broker.port,
                                    mqtt_ws_path, kBrokerRootCAs, "mqtt");
    broker.websocket.setReconnectInterval(3000);
    broker.mqtt.begin(broker.websocket);
    broker.mqtt.setOptions(120, true, 5000);
    broker.next_backoff_ms = millis() + kReconnectDelaysMs[0];
    broker.websocket_started = true;
  }
  broker.websocket.loop();
  updateReconnectState(broker);
  if (broker.mqtt.isConnected() || !broker.websocket.isConnected()) return;
  char client_id[40];
  snprintf(client_id, sizeof(client_id), "meshcore-%c-%.16s",
           broker.name[4], observer_id);
  char offline[480];
  buildStatus(offline, sizeof(offline), "offline");
  broker.mqtt.setWill(status_topic, String(offline), true, 0);
  char username[4 + 2 * PUB_KEY_SIZE + 1];
  snprintf(username, sizeof(username), "v1_%s", observer_id);
  char auth_token[640];
  if (!buildAuthToken(broker.audience, auth_token, sizeof(auth_token))) return;
  const bool connected = broker.mqtt.connect(client_id, username, auth_token);
  if (connected) publishStatus(broker);
}

void publish(BrokerState &broker, const RxItem &item) {
  char raw_hex[2 * kMaxRawPacket + 1];
  toHex(item.raw, item.len, raw_hex);

  uint8_t packet_type = 0;
  uint8_t path_len_wire = 0;
  char route = 'U';
  size_t payload_offset = 0;
  size_t payload_len = 0;
  char hash[17] = "0000000000000000";
  if (parsePacket(item, packet_type, route, path_len_wire, payload_offset, payload_len)) {
    packetHash(item, packet_type, path_len_wire, payload_offset, payload_len, hash);
  }

  char timestamp[25] = "";
  char clock_time[9] = "";
  char date[11] = "";
  utcFields(timestamp, clock_time, date);

  // Full meshcore-packet-capture / mctomqtt-compatible analyzer envelope.
  char json[1152];
  const int n = snprintf(json, sizeof(json),
      "{\"origin\":\"%s\",\"origin_id\":\"%s\",\"type\":\"PACKET\","
      "\"timestamp\":\"%s\",\"direction\":\"rx\",\"time\":\"%s\",\"date\":\"%s\","
      "\"len\":\"%u\",\"packet_type\":\"%u\",\"route\":\"%c\","
      "\"payload_len\":\"%u\",\"raw\":\"%s\",\"SNR\":\"%.1f\","
      "\"RSSI\":\"%.1f\",\"hash\":\"%s\"}",
      observer_name, observer_id, timestamp, clock_time, date,
      static_cast<unsigned>(item.len), static_cast<unsigned>(packet_type), route,
      static_cast<unsigned>(payload_len), raw_hex,
      item.snr_x10 / 10.0f, item.rssi_x10 / 10.0f, hash);
  if (n > 0 && static_cast<size_t>(n) < sizeof(json) &&
      broker.mqtt.publish(packet_topic, String(json), false, 0)) ++broker.published;
}

void observerTask(void *arg) {
  BrokerState &broker = *static_cast<BrokerState *>(arg);
  RxItem item;
  for (;;) {
    // Extremely low-memory fallback if the dedicated Wi-Fi task could not be
    // created. Only one broker task owns the state machine in that case.
    if (!wifi_task_handle && &broker == &broker3) updateWifiState();
    ensureMqtt(broker);
    // TLS/WebSocket setup can take long enough to trip the ESP32 idle watchdog
    // when both broker workers are pinned to the same core.
    vTaskDelay(pdMS_TO_TICKS(10));
    if (wifi_online && broker.websocket_started) broker.mqtt.update();

    if (wifi_online && broker.mqtt.isConnected() &&
        millis() - broker.last_status_ms >= 300000UL) {
      publishStatus(broker);
    }

    if (xQueueReceive(broker.queue, &item, pdMS_TO_TICKS(100)) == pdTRUE) {
      if (wifi_online && broker.mqtt.isConnected()) {
        publish(broker, item);
      } else {
        ++broker.dropped;
        ++dropped;
      }
      // No retry and no blocking storage: mesh routing always has priority.
    }
    vTaskDelay(pdMS_TO_TICKS(1));
  }
}

}  // namespace

namespace CoreScopeObserver {

void begin(const mesh::LocalIdentity &identity, const Config &config) {
  if (broker3.queue || broker4.queue) return;
  local_identity = &identity;
  wifi_ssid = config.wifi_ssid;
  wifi_password = config.wifi_password;
  mqtt_ws_path = config.mqtt_ws_path;
  observer_name = config.observer_name;
  observer_model = config.model;
  snprintf(radio_description, sizeof(radio_description),
           "SX1262 %.3f MHz SF%u BW%.1f CR4/%u",
           config.frequency, static_cast<unsigned>(config.spreading_factor),
           config.bandwidth, static_cast<unsigned>(config.coding_rate));
  broker3.host = config.mqtt3_host;
  broker3.port = config.mqtt3_port;
  broker3.audience = config.mqtt3_audience;
  broker4.host = config.mqtt4_host;
  broker4.port = config.mqtt4_port;
  broker4.audience = config.mqtt4_audience;
  toHex(identity.pub_key, PUB_KEY_SIZE, observer_id);
  snprintf(packet_topic, sizeof(packet_topic), "meshcore/%s/%s/packets", config.iata, observer_id);
  snprintf(status_topic, sizeof(status_topic), "meshcore/%s/%s/status", config.iata, observer_id);
  signing_mutex = xSemaphoreCreateMutex();
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  // Reconnects are managed here so that association and DHCP each have a
  // bounded timeout and cannot remain stuck indefinitely.
  WiFi.setAutoReconnect(false);
  startWifiAttempt();
  configTime(0, 0, "pool.ntp.org", "time.cloudflare.com");
  broker3.queue = xQueueCreate(CORESCOPE_QUEUE_LENGTH, sizeof(RxItem));
  broker4.queue = xQueueCreate(CORESCOPE_QUEUE_LENGTH, sizeof(RxItem));
  if (xTaskCreatePinnedToCore(wifiTask, "corescope-wifi", 4096, nullptr, 1,
                              &wifi_task_handle, 0) != pdPASS) {
    wifi_task_handle = nullptr;
    Serial.println("[CoreScope WiFi] task allocation failed; using MQTT task fallback");
  }
  if (broker3.queue) {
    xTaskCreatePinnedToCore(observerTask, "corescope-mqtt1",
                            CORESCOPE_TASK_STACK_SIZE, &broker3, 1, nullptr, 0);
  }
  if (broker4.queue) {
    xTaskCreatePinnedToCore(observerTask, "corescope-mqtt2",
                            CORESCOPE_TASK_STACK_SIZE, &broker4, 1, nullptr, 1);
  }
}

void enqueueRx(float snr, float rssi, const uint8_t *raw, size_t len) {
  if (!raw || len == 0 || len > kMaxRawPacket) return;
  RxItem item{};
  item.len = static_cast<uint8_t>(len);
  item.snr_x10 = static_cast<int16_t>(snr * 10.0f);
  item.rssi_x10 = static_cast<int16_t>(rssi * 10.0f);
  memcpy(item.raw, raw, len);
  for (BrokerState *broker : brokers) {
    if (!broker->queue || xQueueSend(broker->queue, &item, 0) != pdTRUE) {
      ++broker->dropped;
      ++dropped;
    }
  }
}

uint32_t droppedPackets() { return dropped; }

}  // namespace CoreScopeObserver

#endif
