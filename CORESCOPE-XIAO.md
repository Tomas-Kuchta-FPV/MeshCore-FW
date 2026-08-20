# MeshCore XIAO S3 WIO repeater with CoreScope observer

Target: Seeed Studio XIAO ESP32S3 + Wio-SX1262.

The normal MeshCore repeater remains the primary function. Every received raw
LoRa frame is copied into a bounded FreeRTOS queue without waiting. A separate
low-priority task handles Wi-Fi and MQTT. When the queue or network is
unavailable, observer frames are dropped; routing and retransmission are never
blocked.

## Configure from the MeshCore mobile app

After this firmware has been flashed once, the listed settings are persisted in
the repeater preferences and can be changed from Liam Cottle's MeshCore app.
Open the repeater, log in as administrator, open **Command Line**, and use the
commands below. Compiling the firmware again is not required.

MeshCore's standard repeater commands configure the advertisement identity:

```text
set name XiaoS3 CoreScope
set lat 0.0
set lon 0.0
```

CoreScope observer settings use these commands:

```text
set corescope.wifi.ssid Guest
set corescope.wifi.password -
set corescope.mqtt3.host mqtt1.meshcore.cz
set corescope.mqtt3.port 443
set corescope.mqtt3.audience mqtt1.meshcore.cz
set corescope.mqtt4.host mqtt2.meshcore.website
set corescope.mqtt4.port 443
set corescope.mqtt4.audience mqtt2.meshcore.website
set corescope.ws.path /
set corescope.iata JCL
set corescope.observer.name XIAO CoreScope
reboot
```

Use `-` as the Wi-Fi password for an open network. Network and observer changes
are saved immediately but deliberately applied only after `reboot`, so changing
them cannot interrupt the primary LoRa routing task halfway through an update.

Read the configuration with:

```text
get name
get lat
get lon
get corescope
get corescope.wifi.ssid
get corescope.wifi.password
get corescope.mqtt3.host
get corescope.mqtt3.port
get corescope.mqtt3.audience
get corescope.mqtt4.host
get corescope.mqtt4.port
get corescope.mqtt4.audience
get corescope.ws.path
get corescope.iata
get corescope.observer.name
```

For security, the Wi-Fi password is never returned; the reply is only `(set)`
or `(empty)`. `corescope` or `get corescope.help` displays the available key
groups.

## Configure

The values in the `Xiao_S3_WIO_repeater_corescope` section of
`variants/xiao_s3_wio/platformio.ini`:

- `CORESCOPE_WIFI_SSID`
- `CORESCOPE_WIFI_PASSWORD`
- `CORESCOPE_MQTT3_HOST`, `CORESCOPE_MQTT3_PORT`, `CORESCOPE_MQTT3_TOKEN_AUDIENCE`
- `CORESCOPE_MQTT4_HOST`, `CORESCOPE_MQTT4_PORT`, `CORESCOPE_MQTT4_TOKEN_AUDIENCE`
- `CORESCOPE_MQTT_WS_PATH`
- `CORESCOPE_IATA` (three-letter observer region, currently `JCL`)
- `CORESCOPE_OBSERVER_NAME`

are factory defaults used on the first boot or after preferences are erased.
Normal changes should be made from the mobile app as described above.

Also replace the default MeshCore `ADMIN_PASSWORD`, `ADVERT_LAT`, and
`ADVERT_LON` before deployment.

## Build and upload

```bash
pio run -e Xiao_S3_WIO_repeater_corescope
pio run -e Xiao_S3_WIO_repeater_corescope -t upload --upload-port /dev/ttyACM0
```

For a first-time ESP32 installation, build the merged image:

```bash
pio run -e Xiao_S3_WIO_repeater_corescope -t mergebin
```

## CoreScope input

The firmware publishes meshcoretomqtt-compatible JSON to:

```text
meshcore/{IATA}/{REPEATER_PUBLIC_KEY}/packets
```

Packet payloads follow the `agessaman/meshcore-packet-capture` / mctomqtt
envelope: `origin`, uppercase `origin_id`, UTC `timestamp`, `type`,
`direction`, `time`, `date`, `len`, `packet_type`, `route`, `payload_len`,
uppercase `raw`, `SNR`, `RSSI`, and the MeshCore-compatible 8-byte `hash`.

The firmware starts SNTP after Wi-Fi connects. A valid clock is required for
TLS certificate validation and for the signed MeshCore authentication token;
until SNTP succeeds, only observer uploads are deferred.

The retained status topic is:

```text
meshcore/{IATA}/{REPEATER_PUBLIC_KEY}/status
```

It announces online/offline state, hardware, firmware, radio, repeater
capability and uptime. Both WebSocket connections use a 120-second keep-alive.

CoreScope must subscribe to `meshcore/#` or at least
`meshcore/+/+/packets`. The broker must be reachable from the XIAO over Wi-Fi.

## Design limits

- MQTT Broker 3 uses `wss://mqtt1.meshcore.cz:443/` with audience
  `mqtt1.meshcore.cz`.
- MQTT Broker 4 uses `wss://mqtt2.meshcore.website:443/` with audience
  `mqtt2.meshcore.website`.
- Each broker has its own WebSocket/MQTT client, JWT, queue and FreeRTOS task;
  failure of one destination does not stop delivery to the other.
- After a connection loss, the MQTT state is explicitly rebuilt so the next
  WebSocket session always sends a fresh MQTT CONNECT and JWT. Reconnect delay
  backs off through 3, 6, 12, 30 and 60 seconds; a successful connection resets
  it to 3 seconds.
- MQTT authentication uses a 24-hour JWT signed locally by the repeater's
  MeshCore identity. The private key never leaves the device.
- No received packet is persisted while Wi-Fi/MQTT is unavailable.
- Each broker queue defaults to 12 packets and can be changed with
  `CORESCOPE_QUEUE_LENGTH`.
- Each MQTT/TLS task uses a 16 kB stack by default. It can be adjusted with
  `CORESCOPE_TASK_STACK_SIZE`; reducing it below 16 kB is not recommended for
  ESP32-S3 TLS connections.
