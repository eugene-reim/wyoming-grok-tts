# wyoming-grok-tts

### Wyoming protocol Text-to-Speech server that uses the xAI Grok TTS API.

Supports full streaming (`synthesize-start` / `synthesize-chunk` / `synthesize-stop`) for low-latency playback on Home Assistant satellites and Voice Preview Edition devices.

The service receives text via the Wyoming protocol, splits it into sentences, synthesizes each sentence with the xAI TTS API (PCM 24 kHz), and streams the audio back to the client as soon as it is ready.

## Features

- Full Wyoming streaming support (ideal for satellites / HAVPE)
- Sentence-boundary synthesis for low time-to-first-audio
- In-memory LRU cache (optional)
- Speech tags support (pass-through to xAI)

## Requirements

- Docker
- xAI API key

## Configuration

| Variable         | Default                   | Required | Description                              |
|------------------|---------------------------|----------|------------------------------------------|
| `XAI_API_KEY`    | —                         | Yes      | xAI API key                              |
| `WYOMING_URI`    | `tcp://0.0.0.0:10600`     | No       | Address the Wyoming server listens on    |
| `DEBUG`          | `false`                   | No       | Enable debug logging                     |
| `CACHE_SIZE`     | `64`                      | No       | LRU cache size (0 = disabled)            |
| `XAI_TTS_URL`    | `https://api.x.ai/v1/tts` | No       | xAI TTS endpoint                         |

Supported languages: see [xAI documentation](https://docs.x.ai/developers/model-capabilities/audio/text-to-speech#supported-languages).
Supported voices: see [xAI documentation](https://docs.x.ai/developers/model-capabilities/audio/text-to-speech#voices).

## Run with Docker

```bash
docker run -d \
  --name wyoming-grok-tts \
  -p 10600:10600 \
  -e XAI_API_KEY=your_api_key_here \
  ghcr.io/eugene-reim/wyoming-grok-tts:latest
```

Build locally:

```bash
docker build -t wyoming-grok-tts .
docker run -d \
  --name wyoming-grok-tts \
  -p 10600:10600 \
  -e XAI_API_KEY=your_api_key_here \
  wyoming-grok-tts
```

## Run with Docker Compose

```yaml
services:
  wyoming-grok-tts:
    image: ghcr.io/eugene-reim/wyoming-grok-tts:latest
    container_name: wyoming-grok-tts
    ports:
      - "10600:10600"
    environment:
      - XAI_API_KEY=${XAI_API_KEY}
      - CACHE_SIZE=64
    restart: unless-stopped
```

```bash
export XAI_API_KEY=your_api_key_here
docker compose up -d
```

