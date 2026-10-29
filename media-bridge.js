const mqtt = require("mqtt");
const { execFile } = require("child_process");
const { promisify } = require("util");

const execFileAsync = promisify(execFile);

const MQTT_HOST = "mqtt://not-a-server.little-tyrannosaurus.ts.net";
const MQTT_USER = "";
const MQTT_PASS = "";

const BASE = "media/workstation";

const ACTIVE_POLL_MS = 5000;
const IDLE_POLL_MS = 15000;

const client = mqtt.connect(MQTT_HOST, {
  username: MQTT_USER || undefined,
  password: MQTT_PASS || undefined,
  reconnectPeriod: 5000,
});

let lastPublished = {};
let polling = false;
let pollTimer = null;
let shuttingDown = false;

// Run a command and return stdout.
function run(command, args, timeout = 5000) {
  return execFileAsync(command, args, { timeout });
}

// Read the current system volume.
async function getSystemVolume() {
  try {
    const { stdout } = await run("wpctl", [
      "get-volume",
      "@DEFAULT_AUDIO_SINK@",
    ]);

    const match = stdout.match(/Volume:\s+([0-9.]+)/);

    return match ? Number(match[1]) : 0;
  } catch (err) {
    console.error("wpctl volume error:", err.message);
    return null;
  }
}

// Publish only values that have actually changed.
// Position is intentionally published less frequently.
function publishState(data) {
  const topics = {
    state: data.state,
    player: data.player,
    title: data.title,
    artist: data.artist,
    album: data.album,
    volume: data.volume,
    length: data.length,
    position: data.position,
  };

  for (const [key, value] of Object.entries(topics)) {
    if (value === null || value === undefined) {
      continue;
    }

    const payload = String(value);

    if (lastPublished[key] === payload) {
      continue;
    }

    lastPublished[key] = payload;

    client.publish(
      `${BASE}/${key}`,
      payload,
      { retain: true },
      (err) => {
        if (err) {
          console.error(`MQTT publish failed (${key}):`, err.message);
        }
      }
    );
  }
}

// Read the current player state.
// A failed query does not automatically mean the player is idle.
async function getState() {
  if (polling || !client.connected || shuttingDown) {
    return;
  }

  polling = true;

  let nextInterval = ACTIVE_POLL_MS;

  try {
    const { stdout } = await run("playerctl", [
      "-a",
      "metadata",
      "--format",
      JSON.stringify({
        player: "{{playerName}}",
        status: "{{status}}",
        title: "{{title}}",
        artist: "{{artist}}",
        album: "{{album}}",
        length: "{{mpris:length}}",
        position: "{{position}}",
      }),
    ]);

    const lines = stdout.split("\n").filter(Boolean);

    // No available players: publish idle once, then back off.
    if (lines.length === 0) {
      const volume = await getSystemVolume();

      publishState({
        state: "idle",
        player: "",
        title: "",
        artist: "",
        album: "",
        volume,
        length: 0,
        position: 0,
      });

      nextInterval = IDLE_POLL_MS;
      return;
    }

    // Prefer a playing player, then a paused player.
    // Fall back to the first available player.
    const players = lines.map((line) => JSON.parse(line));

    const data =
      players.find(
        (player) => player.status?.toLowerCase() === "playing"
      ) ||
      players.find(
        (player) => player.status?.toLowerCase() === "paused"
      ) ||
      players[0];

    const status = data.status?.toLowerCase() || "idle";

    const volume = await getSystemVolume();

    publishState({
      state: status,
      player: data.player || "",
      title: data.title || "",
      artist: data.artist || "",
      album: data.album || "",
      volume,
      length: Math.floor(Number(data.length) / 1000000) || 0,
      position: Math.floor(Number(data.position) / 1000000) || 0,
    });

    // Poll less frequently when playback isn't active.
    nextInterval =
      status === "playing" ? ACTIVE_POLL_MS : IDLE_POLL_MS;

  } catch (err) {
    // Don't overwrite valid state with empty values on transient failures.
    console.error("State update failed:", err.message);

    nextInterval = IDLE_POLL_MS;
  } finally {
    polling = false;

    if (!shuttingDown && client.connected) {
      clearTimeout(pollTimer);
      pollTimer = setTimeout(getState, nextInterval);
    }
  }
}

// Start a fresh polling cycle.
function schedulePoll(delay = 0) {
  clearTimeout(pollTimer);
  pollTimer = setTimeout(getState, delay);
}

// Handle MQTT connection.
client.on("connect", () => {
  console.log("Connected to MQTT");

  client.subscribe(`${BASE}/command/#`, (err) => {
    if (err) {
      console.error("MQTT subscribe error:", err.message);
      return;
    }

    console.log(`Subscribed to ${BASE}/command/#`);
  });

  // Force a fresh state publication after reconnecting.
  lastPublished = {};

  schedulePoll();
});

// Handle MQTT commands.
client.on("message", async (topic, message) => {
  const payload = message.toString().trim();
  const commandTopic = topic.replace(`${BASE}/command/`, "");

  console.log(`Command: ${commandTopic}`, payload);

  const commands = {
    play: ["play"],
    pause: ["pause"],
    next: ["next"],
    previous: ["previous"],
    stop: ["stop"],
  };

  if (commands[commandTopic]) {
    try {
      await run("playerctl", ["-a", ...commands[commandTopic]]);
      schedulePoll(500);
    } catch (err) {
      console.error(`Command failed: ${commandTopic}`, err.message);
    }

    return;
  }

  if (commandTopic === "volume") {
    const volume = Number(payload);

    if (!Number.isFinite(volume)) {
      console.error("Invalid volume:", payload);
      return;
    }

    const clamped = Math.max(0, Math.min(1, volume));

    try {
      await run("wpctl", [
        "set-volume",
        "@DEFAULT_AUDIO_SINK@",
        String(clamped),
      ]);

      console.log(`System volume set to ${clamped}`);

      schedulePoll(250);
    } catch (err) {
      console.error("Volume command failed:", err.message);
    }
  }
});

// MQTT errors.
client.on("error", (err) => {
  console.error("MQTT error:", err.message);
});

// Clean shutdown.
function shutdown() {
  if (shuttingDown) return;

  shuttingDown = true;
  clearTimeout(pollTimer);

  client.end(false, {}, () => {
    process.exit(0);
  });

  setTimeout(() => process.exit(1), 2000).unref();
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

console.log("Media bridge starting...");
