#!/usr/bin/env python3
"""
Wyoming TTS server that uses the xAI Grok Text-to-Speech API.

Supports full streaming (synthesize-start / chunk / stop) for low-latency
playback on Home Assistant satellites and Voice Preview Edition devices.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
from collections import OrderedDict
from functools import partial
from typing import Optional

import httpx
from wyoming.audio import AudioChunk, AudioStart, AudioStop
from wyoming.event import Event
from wyoming.info import Attribution, Describe, Info, TtsProgram, TtsVoice
from wyoming.server import AsyncEventHandler, AsyncServer
from wyoming.tts import (
    Synthesize,
    SynthesizeChunk,
    SynthesizeStart,
    SynthesizeStop,
    SynthesizeStopped,
)

_LOGGER = logging.getLogger("wyoming-grok-tts")

# ====================== DEFAULT ENVS ======================
XAI_API_KEY = os.getenv("XAI_API_KEY", "")
XAI_TTS_URL = os.getenv("XAI_TTS_URL", "https://api.x.ai/v1/tts")
DEFAULT_VOICE = os.getenv("DEFAULT_VOICE", "eve")
DEFAULT_LANGUAGE = os.getenv("LANGUAGE", "auto")
URI = os.getenv("WYOMING_URI", "tcp://0.0.0.0:10600")
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")
CACHE_SIZE = int(os.getenv("CACHE_SIZE", "64"))  # 0 = disabled

SAMPLE_RATE = 24000
SAMPLE_WIDTH = 2
CHANNELS = 1
CHUNK_SIZE = 4096

_LANGS = ["en", "ru", "auto", "de", "fr", "es", "it", "pt", "ja", "ko", "zh"]
_ATTR = Attribution(name="xAI", url="https://x.ai")
VOICES = [
    TtsVoice(
        name="eve",
        description="Eve — Energetic and upbeat",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="ara",
        description="Ara — Warm and friendly",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="leo",
        description="Leo — Authoritative and strong",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="rex",
        description="Rex — Confident and clear",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="sal",
        description="Sal — Smooth and balanced",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="carina",
        description="Carina — Soft, empathetic, soothing",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="zagan",
        description="Zagan — Powerful, dramatic",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="helix",
        description="Helix — Bold, dynamic, adrenaline-fueled",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="orion",
        description="Orion — Rich, cinematic, resonant",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="luna",
        description="Luna — Gentle, patient, nurturing",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="iris",
        description="Iris — Friendly, upbeat, charming",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="altair",
        description="Altair — Elegant, refined, premium",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="zenith",
        description="Zenith — Sharp, focused, driven",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="perseus",
        description="Perseus — Strong, confident, trustworthy",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="helios",
        description="Helios — Upbeat, energetic, versatile",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="lux",
        description="Lux — Grounded, calm, wise",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="kepler",
        description="Kepler — Inventive, charismatic",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="rigel",
        description="Rigel — Precise, professional, confident",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="cosmo",
        description="Cosmo — Bright, curious, easy to follow",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="celeste",
        description="Celeste — Compassionate, reassuring",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="ursa",
        description="Ursa — Friendly, warm, steadfast",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="sirius",
        description="Sirius — Quick-witted, playful",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="lumen",
        description="Lumen — Warm, articulate, engaging",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="castor",
        description="Castor — Charismatic, easygoing",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="naksh",
        description="Naksh — Warm, thoughtful, wise",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
    TtsVoice(
        name="atlas",
        description="Atlas — Confident, commanding, reassuring",
        installed=True,
        version="1.0.0",
        languages=_LANGS,
        attribution=_ATTR,
    ),
]


class LRUCache:
    """Simple LRU cache for synthesized audio."""

    def __init__(self, maxsize: int = 64):
        self.maxsize = maxsize
        self._cache: OrderedDict[str, bytes] = OrderedDict()

    def get(self, key: str) -> Optional[bytes]:
        if key not in self._cache:
            return None
        self._cache.move_to_end(key)
        return self._cache[key]

    def put(self, key: str, value: bytes) -> None:
        if self.maxsize <= 0:
            return
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = value
        if len(self._cache) > self.maxsize:
            self._cache.popitem(last=False)


class SentenceBoundaryDetector:
    """Lightweight sentence splitter that splits text into sentences based on punctuation."""

    _SENTENCE_END = re.compile(r"(?<=[.!?…])\s+")

    def __init__(self) -> None:
        self._buffer = ""

    def add_chunk(self, text: str) -> list[str]:
        self._buffer += text
        sentences: list[str] = []
        while True:
            match = self._SENTENCE_END.search(self._buffer)
            if not match:
                break
            end = match.end()
            sentence = self._buffer[:end].strip()
            if sentence:
                sentences.append(sentence)
            self._buffer = self._buffer[end:]
        return sentences

    def finish(self) -> str:
        remaining = self._buffer.strip()
        self._buffer = ""
        return remaining


class GrokTtsHandler(AsyncEventHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = httpx.AsyncClient(timeout=60.0)
        self._cache = LRUCache(CACHE_SIZE)

        # Streaming state
        self._is_streaming = False
        self._voice_id = DEFAULT_VOICE
        self._language = DEFAULT_LANGUAGE
        self._sbd = SentenceBoundaryDetector()
        self._audio_started = False

    async def handle_event(self, event: Event) -> bool:
        if Describe.is_type(event.type):
            await self.write_event(
                Info(
                    tts=[
                        TtsProgram(
                            name="grok-tts",
                            description="Grok Text-to-Speech (xAI)",
                            attribution=Attribution(name="xAI", url="https://x.ai"),
                            installed=True,
                            version="1.0.0",
                            voices=VOICES,
                            supports_synthesize_streaming=True,
                        )
                    ]
                ).event()
            )
            return True

        # ---- Streaming path ----
        if SynthesizeStart.is_type(event.type):
            start = SynthesizeStart.from_event(event)
            self._is_streaming = True
            self._audio_started = False
            self._sbd = SentenceBoundaryDetector()

            self._voice_id = DEFAULT_VOICE
            self._language = DEFAULT_LANGUAGE
            if start.voice:
                if start.voice.name:
                    self._voice_id = start.voice.name.lower()
                if getattr(start.voice, "language", None):
                    self._language = start.voice.language

            _LOGGER.debug(
                "Stream started: voice=%s language=%s", self._voice_id, self._language
            )
            return True

        if SynthesizeChunk.is_type(event.type) and self._is_streaming:
            chunk = SynthesizeChunk.from_event(event)
            for sentence in self._sbd.add_chunk(chunk.text):
                await self._synthesize_and_send(
                    sentence,
                    voice_id=self._voice_id,
                    language=self._language,
                    send_start=not self._audio_started,
                    send_stop=False,
                )
                self._audio_started = True
            return True

        if SynthesizeStop.is_type(event.type) and self._is_streaming:
            remaining = self._sbd.finish()
            if remaining:
                await self._synthesize_and_send(
                    remaining,
                    voice_id=self._voice_id,
                    language=self._language,
                    send_start=not self._audio_started,
                    send_stop=True,
                )
            elif self._audio_started:
                await self.write_event(AudioStop().event())

            self._is_streaming = False
            self._audio_started = False
            await self.write_event(SynthesizeStopped().event())
            _LOGGER.debug("Stream stopped")
            return True

        # ---- Non-streaming / backwards-compatible path ----
        if Synthesize.is_type(event.type):
            # If we are inside a streaming session, this is the
            # backwards-compatibility full-text event – ignore it.
            if self._is_streaming:
                return True

            synthesize = Synthesize.from_event(event)
            text = (synthesize.text or "").strip()
            if not text:
                return True

            voice_id = DEFAULT_VOICE
            language = DEFAULT_LANGUAGE
            if synthesize.voice:
                if synthesize.voice.name:
                    voice_id = synthesize.voice.name.lower()
                if getattr(synthesize.voice, "language", None):
                    language = synthesize.voice.language

            # Split into sentences for progressive playback even in non-stream mode
            sbd = SentenceBoundaryDetector()
            sentences = sbd.add_chunk(text)
            remaining = sbd.finish()
            if remaining:
                sentences.append(remaining)

            if not sentences:
                return True

            for i, sentence in enumerate(sentences):
                await self._synthesize_and_send(
                    sentence,
                    voice_id=voice_id,
                    language=language,
                    send_start=(i == 0),
                    send_stop=(i == len(sentences) - 1),
                )
            return True

        return True

    async def _synthesize_and_send(
        self,
        text: str,
        voice_id: str,
        language: str,
        send_start: bool = True,
        send_stop: bool = True,
    ) -> None:
        text = text.strip()
        if not text:
            return

        audio_bytes = await self._get_audio(text, voice_id, language)
        if not audio_bytes:
            return

        if send_start:
            await self.write_event(
                AudioStart(
                    rate=SAMPLE_RATE, width=SAMPLE_WIDTH, channels=CHANNELS
                ).event()
            )

        for i in range(0, len(audio_bytes), CHUNK_SIZE):
            chunk = audio_bytes[i : i + CHUNK_SIZE]
            await self.write_event(
                AudioChunk(
                    audio=chunk,
                    rate=SAMPLE_RATE,
                    width=SAMPLE_WIDTH,
                    channels=CHANNELS,
                ).event()
            )

        if send_stop:
            await self.write_event(AudioStop().event())

        _LOGGER.info(
            "Sent audio voice=%s lang=%s text=%.60s... (%d bytes)",
            voice_id,
            language,
            text,
            len(audio_bytes),
        )

    async def _get_audio(self, text: str, voice_id: str, language: str) -> bytes:
        cache_key = hashlib.sha256(f"{voice_id}:{language}:{text}".encode()).hexdigest()

        cached = self._cache.get(cache_key)
        if cached is not None:
            _LOGGER.debug("Cache hit (%d bytes)", len(cached))
            return cached

        audio = await self._call_xai(text, voice_id, language)
        if audio:
            self._cache.put(cache_key, audio)
        return audio

    async def _call_xai(self, text: str, voice_id: str, language: str) -> bytes:
        if not XAI_API_KEY:
            _LOGGER.error("XAI_API_KEY is not set")
            return b""

        headers = {
            "Authorization": f"Bearer {XAI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "text": text,
            "voice_id": voice_id,
            "language": language,
            "output_format": {
                "codec": "pcm",
                "sample_rate": SAMPLE_RATE,
            },
            "optimize_streaming_latency": 1,
        }

        try:
            resp = await self._client.post(
                XAI_TTS_URL,
                headers=headers,
                json=payload,
            )
            if resp.status_code != 200:
                _LOGGER.error("xAI TTS error %s: %s", resp.status_code, resp.text[:500])
                return b""
            return resp.content
        except Exception:
            _LOGGER.exception("Failed to call xAI TTS")
            return b""


async def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG if DEBUG else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    _LOGGER.info("Starting wyoming-grok-tts")
    _LOGGER.info("URI: %s", URI)
    _LOGGER.info("Default voice: %s", DEFAULT_VOICE)
    _LOGGER.info("Default language: %s", DEFAULT_LANGUAGE)
    _LOGGER.info("Cache size: %d", CACHE_SIZE)

    if not XAI_API_KEY:
        _LOGGER.warning("XAI_API_KEY is not set – synthesis will fail")

    server = AsyncServer.from_uri(URI)
    await server.run(partial(GrokTtsHandler))


if __name__ == "__main__":
    asyncio.run(main())
