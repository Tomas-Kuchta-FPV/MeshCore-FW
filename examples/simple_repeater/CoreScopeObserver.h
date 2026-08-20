#pragma once

#if defined(WITH_CORESCOPE_OBSERVER) && defined(ESP32)

#include <Arduino.h>
#include <Mesh.h>

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

namespace CoreScopeObserver {

struct Config {
  const char *wifi_ssid;
  const char *wifi_password;
  const char *mqtt3_host;
  uint16_t mqtt3_port;
  const char *mqtt3_audience;
  const char *mqtt4_host;
  uint16_t mqtt4_port;
  const char *mqtt4_audience;
  const char *mqtt_ws_path;
  const char *iata;
  const char *observer_name;
};

void begin(const mesh::LocalIdentity &identity, const Config &config);
void enqueueRx(float snr, float rssi, const uint8_t *raw, size_t len);
uint32_t droppedPackets();

}  // namespace CoreScopeObserver

#endif
