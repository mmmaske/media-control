# media-bridge
Small script to publish media metadata to MQTT topic. Need to set up the variables in `media-bridge.js` as well as the paths in the `media-bridge.service` file to reflect the correct paths in your system.

HomeAssistant should pick up the MQTT topics when setting up the entity from the UI.

# Service install
run `sudo cp media-bridge.service /etc/systemd/system/`
