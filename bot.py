import asyncio
import os
import random
import traceback
from collections import deque
from dataclasses import dataclass, field

import aiosqlite
import discord
import yt_dlp
import spotipy
import yandex_music
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv


load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
YANDEX_MUSIC_TOKEN = os.getenv("YANDEX_MUSIC_TOKEN")
GUILD_ID = 661168864523976716

DATABASE_PATH = "/opt/discord_bot/music_bot.db"
TEST_SOUND_PATH = "/opt/discord_bot/sounds/test.mp3"
IDLE_DISCONNECT_SECONDS = 120
MAX_HISTORY_ITEMS = 20
MAX_FAVORITES_SHOWN = 20
MAX_PLAYLIST_TRACKS = 100
MAX_MATCH_CHOICES = 5
MAX_QUEUE_SIZE = 100

if not TOKEN:
    raise RuntimeError("В файле .env не найден DISCORD_TOKEN")

spotify = None
yandex_client = None
if SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
    spotify = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
        )
    )
else:
    print("Spotify отключён: добавь SPOTIFY_CLIENT_ID и SPOTIFY_CLIENT_SECRET в .env")

if YANDEX_MUSIC_TOKEN:
    try:
        yandex_client = yandex_music.Client(YANDEX_MUSIC_TOKEN).init()
    except Exception as error:
        print(f"Яндекс Музыка отключена: {error!r}")
        yandex_client = None
else:
    print("Яндекс Музыка отключена: добавь YANDEX_MUSIC_TOKEN в .env")


YTDL_OPTIONS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
}

SEARCH_OPTIONS = {
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
}

FFMPEG_OPTIONS = {
    "before_options": (
        "-reconnect 1 -reconnect_streamed 1 "
        "-reconnect_delay_max 5"
    ),
    "options": "-vn",
}


@dataclass
class Track:
    title: str
    url: str
    requested_by: str
    source_name: str = "Прямая ссылка"
    original_query: str = ""


@dataclass
class MatchCandidate:
    title: str
    url: str
    source_name: str
    duration_text: str = ""
    uploader: str = ""


@dataclass
class PendingSelection:
    original_title: str
    query: str
    candidates: list[MatchCandidate] = field(default_factory=list)


queues: dict[int, deque[Track]] = {}
current_tracks: dict[int, Track] = {}
track_details: dict[int, dict] = {}
queue_locks: dict[int, asyncio.Lock] = {}
volumes: dict[int, float] = {}
repeat_modes: dict[int, str] = {}
idle_disconnect_tasks: dict[int, asyncio.Task] = {}
skip_requested: set[int] = set()
panel_locations: dict[int, tuple[int, int]] = {}
panel_locks: dict[int, asyncio.Lock] = {}


def get_queue(guild_id: int) -> deque[Track]:
    if guild_id not in queues:
        queues[guild_id] = deque()

    return queues[guild_id]


def get_lock(guild_id: int) -> asyncio.Lock:
    if guild_id not in queue_locks:
        queue_locks[guild_id] = asyncio.Lock()

    return queue_locks[guild_id]


def get_panel_lock(guild_id: int) -> asyncio.Lock:
    if guild_id not in panel_locks:
        panel_locks[guild_id] = asyncio.Lock()
    return panel_locks[guild_id]


def get_volume(guild_id: int) -> float:
    if guild_id not in volumes:
        volumes[guild_id] = 0.5

    return volumes[guild_id]


def get_repeat_mode(guild_id: int) -> str:
    if guild_id not in repeat_modes:
        repeat_modes[guild_id] = "off"

    return repeat_modes[guild_id]


def get_repeat_text(guild_id: int) -> str:
    mode = get_repeat_mode(guild_id)

    if mode == "track":
        return "🔂 Повтор трека"

    if mode == "queue":
        return "🔁 Повтор очереди"

    return "⏹ Повтор выключен"


def format_duration(seconds) -> str:
    if not seconds:
        return "Неизвестно"

    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}:{minutes:02}:{seconds:02}"

    return f"{minutes}:{seconds:02}"


async def init_database():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                volume REAL NOT NULL DEFAULT 0.5,
                repeat_mode TEXT NOT NULL DEFAULT 'off'
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS music_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                position INTEGER NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                requested_by TEXT NOT NULL
            )
        """)

        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_music_queue_guild_position
            ON music_queue(guild_id, position)
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                added_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(guild_id, user_id, url)
            )
        """)

        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_favorites_user
            ON favorites(guild_id, user_id, id DESC)
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS play_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                requested_by TEXT NOT NULL,
                played_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_history_guild
            ON play_history(guild_id, id DESC)
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS music_panels (
                guild_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL
            )
        """)

        await db.commit()


async def load_saved_data():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("""
            SELECT guild_id, volume, repeat_mode
            FROM guild_settings
        """) as cursor:
            async for guild_id, volume, repeat_mode in cursor:
                volumes[guild_id] = volume
                repeat_modes[guild_id] = repeat_mode

        async with db.execute("""
            SELECT guild_id, title, url, requested_by
            FROM music_queue
            ORDER BY guild_id, position, id
        """) as cursor:
            async for guild_id, title, url, requested_by in cursor:
                get_queue(guild_id).append(
                    Track(
                        title=title,
                        url=url,
                        requested_by=requested_by,
                    )
                )

        async with db.execute("""
            SELECT guild_id, channel_id, message_id
            FROM music_panels
        """) as cursor:
            async for guild_id, channel_id, message_id in cursor:
                panel_locations[guild_id] = (
                    channel_id,
                    message_id,
                )

    print("SQLite: очередь, громкость и настройки восстановлены.")


async def save_guild_settings(guild_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT INTO guild_settings (
                guild_id,
                volume,
                repeat_mode
            )
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id)
            DO UPDATE SET
                volume = excluded.volume,
                repeat_mode = excluded.repeat_mode
        """, (
            guild_id,
            get_volume(guild_id),
            get_repeat_mode(guild_id),
        ))

        await db.commit()


async def save_queue(guild_id: int):
    queue_items = list(get_queue(guild_id))

    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM music_queue WHERE guild_id = ?",
            (guild_id,),
        )

        for position, track in enumerate(queue_items, start=1):
            await db.execute("""
                INSERT INTO music_queue (
                    guild_id,
                    position,
                    title,
                    url,
                    requested_by
                )
                VALUES (?, ?, ?, ?, ?)
            """, (
                guild_id,
                position,
                track.title,
                track.url,
                track.requested_by,
            ))

        await db.commit()


async def save_panel_location(
    guild_id: int,
    channel_id: int,
    message_id: int,
):
    panel_locations[guild_id] = (channel_id, message_id)

    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT INTO music_panels (
                guild_id,
                channel_id,
                message_id
            )
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id)
            DO UPDATE SET
                channel_id = excluded.channel_id,
                message_id = excluded.message_id
        """, (
            guild_id,
            channel_id,
            message_id,
        ))

        await db.commit()


async def delete_panel_location(guild_id: int):
    panel_locations.pop(guild_id, None)

    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM music_panels WHERE guild_id = ?",
            (guild_id,),
        )

        await db.commit()


async def add_history_item(guild_id: int, track: Track):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT INTO play_history (
                guild_id,
                title,
                url,
                requested_by
            )
            VALUES (?, ?, ?, ?)
        """, (
            guild_id,
            track.title,
            track.url,
            track.requested_by,
        ))

        await db.execute("""
            DELETE FROM play_history
            WHERE guild_id = ?
            AND id NOT IN (
                SELECT id
                FROM play_history
                WHERE guild_id = ?
                ORDER BY id DESC
                LIMIT ?
            )
        """, (
            guild_id,
            guild_id,
            MAX_HISTORY_ITEMS,
        ))

        await db.commit()


async def add_favorite(
    guild_id: int,
    user: discord.abc.User,
    track: Track,
) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            INSERT OR IGNORE INTO favorites (
                guild_id,
                user_id,
                user_name,
                title,
                url
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            guild_id,
            user.id,
            user.display_name,
            track.title,
            track.url,
        ))

        await db.commit()

        return cursor.rowcount > 0


async def get_favorites(
    guild_id: int,
    user_id: int,
) -> list[Track]:
    tracks = []

    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("""
            SELECT title, url, user_name
            FROM favorites
            WHERE guild_id = ?
            AND user_id = ?
            ORDER BY id DESC
            LIMIT ?
        """, (
            guild_id,
            user_id,
            MAX_FAVORITES_SHOWN,
        )) as cursor:
            async for title, url, user_name in cursor:
                tracks.append(
                    Track(
                        title=title,
                        url=url,
                        requested_by=user_name,
                    )
                )

    return tracks


async def get_history(guild_id: int) -> list[tuple]:
    items = []

    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("""
            SELECT title, requested_by, played_at
            FROM play_history
            WHERE guild_id = ?
            ORDER BY id DESC
            LIMIT ?
        """, (
            guild_id,
            MAX_HISTORY_ITEMS,
        )) as cursor:
            async for title, requested_by, played_at in cursor:
                items.append((
                    title,
                    requested_by,
                    played_at,
                ))

    return items


def get_audio_info(url: str) -> dict:
    with yt_dlp.YoutubeDL(YTDL_OPTIONS) as ydl:
        info = ydl.extract_info(url, download=False)

        if "entries" in info:
            info = next(entry for entry in info["entries"] if entry)

        return info


def search_videos(query: str) -> list[dict]:
    with yt_dlp.YoutubeDL(SEARCH_OPTIONS) as ydl:
        result = ydl.extract_info(
            f"ytsearch5:{query}",
            download=False,
        )

    return [
        entry
        for entry in result.get("entries", [])
        if entry and entry.get("webpage_url")
    ]

def is_spotify_url(url: str) -> bool:
    return "open.spotify.com/" in url or url.startswith("spotify:")


def is_yandex_music_url(url: str) -> bool:
    return "music.yandex." in url


def yandex_track_to_query(track) -> tuple[str, str] | None:
    if not track:
        return None

    title = getattr(track, "title", None)
    artists_data = getattr(track, "artists", None) or []
    artists = ", ".join(artist.name for artist in artists_data if getattr(artist, "name", None))
    if not title or not artists:
        return None

    return f"{artists} - {title} audio", f"{artists} - {title}"


def extract_yandex_ids(url: str) -> tuple[str | None, str | None]:
    parts = [part for part in url.split("?")[0].split("/") if part]
    album_id = None
    track_id = None
    for index, part in enumerate(parts):
        if part == "album" and index + 1 < len(parts):
            album_id = parts[index + 1]
        if part == "track" and index + 1 < len(parts):
            track_id = parts[index + 1]
    return album_id, track_id


def get_yandex_tracks(url: str) -> tuple[str, list[tuple[str, str]]]:
    if yandex_client is None:
        raise RuntimeError(
            "Яндекс Музыка не настроена: добавь YANDEX_MUSIC_TOKEN в .env "
            "и перезапусти бота."
        )

    album_id, track_id = extract_yandex_ids(url)

    if track_id:
        tracks = yandex_client.tracks([track_id])
        if not tracks:
            raise RuntimeError("Трек Яндекс Музыки не найден.")
        track = yandex_track_to_query(tracks[0])
        return "трек", [track] if track else []

    if album_id:
        album = yandex_client.albums_with_tracks(album_id)
        tracks = []
        for volume in getattr(album, "volumes", []) or []:
            for item in volume:
                track = yandex_track_to_query(item)
                if track:
                    tracks.append(track)
        return "альбом", tracks[:MAX_PLAYLIST_TRACKS]

    raise RuntimeError(
        "Поддерживаются ссылки Яндекс Музыки на трек или альбом. "
        "Плейлисты можно добавить позже отдельно."
    )


def spotify_item_to_query(item: dict) -> tuple[str, str] | None:
    if not item or item.get("is_local"):
        return None

    name = item.get("name")
    artists = ", ".join(artist["name"] for artist in item.get("artists", []))
    if not name or not artists:
        return None

    return f"{artists} - {name} audio", f"{artists} - {name}"


def get_spotify_tracks(url: str) -> tuple[str, list[tuple[str, str]]]:
    if spotify is None:
        raise RuntimeError(
            "Spotify не настроен: проверь SPOTIFY_CLIENT_ID и "
            "SPOTIFY_CLIENT_SECRET в .env и перезапусти бота."
        )

    if "/track/" in url or url.startswith("spotify:track:"):
        item = spotify.track(url)
        track = spotify_item_to_query(item)
        return "трек", [track] if track else []

    if "/album/" in url or url.startswith("spotify:album:"):
        album = spotify.album(url)
        items = album["tracks"]["items"]
        while album["tracks"].get("next") and len(items) < MAX_PLAYLIST_TRACKS:
            album["tracks"] = spotify.next(album["tracks"])
            items.extend(album["tracks"]["items"])
        return "альбом", [track for item in items[:MAX_PLAYLIST_TRACKS] if (track := spotify_item_to_query(item))]

    if "/playlist/" in url or url.startswith("spotify:playlist:"):
        page = spotify.playlist_items(url, additional_types=("track",), limit=100)
        items = page["items"]
        while page.get("next") and len(items) < MAX_PLAYLIST_TRACKS:
            page = spotify.next(page)
            items.extend(page["items"])
        tracks = []
        for entry in items[:MAX_PLAYLIST_TRACKS]:
            track = spotify_item_to_query(entry.get("track"))
            if track:
                tracks.append(track)
        return "плейлист", tracks

    raise RuntimeError("Поддерживаются ссылки Spotify на трек, альбом или плейлист.")


def search_soundcloud(query: str) -> list[dict]:
    options = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
    }
    with yt_dlp.YoutubeDL(options) as ydl:
        result = ydl.extract_info(f"scsearch1:{query}", download=False)

    return [
        entry
        for entry in result.get("entries", [])
        if entry and entry.get("webpage_url")
    ]



def build_candidates(query: str) -> list[MatchCandidate]:
    candidates: list[MatchCandidate] = []
    seen_urls: set[str] = set()

    youtube_results = search_videos(query)
    for entry in youtube_results[:MAX_MATCH_CHOICES]:
        url = entry.get("webpage_url")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        candidates.append(
            MatchCandidate(
                title=entry.get("title", query),
                url=url,
                source_name="YouTube",
                duration_text=entry.get("duration_string", ""),
                uploader=entry.get("uploader", ""),
            )
        )

    soundcloud_results = search_soundcloud(query)
    for entry in soundcloud_results[:MAX_MATCH_CHOICES]:
        url = entry.get("webpage_url")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        candidates.append(
            MatchCandidate(
                title=entry.get("title", query),
                url=url,
                source_name="SoundCloud",
                duration_text=entry.get("duration_string", ""),
                uploader=entry.get("uploader", ""),
            )
        )

    return candidates

def get_now_playing_text(guild_id: int) -> str:
    track = current_tracks.get(guild_id)

    if not track:
        return "Сейчас ничего не проигрывается."

    return (
        f"Сейчас играет: **{track.title}**\n"
        f"Добавил: {track.requested_by}"
    )


def get_queue_text(guild_id: int) -> str:
    current = current_tracks.get(guild_id)
    queue_items = list(get_queue(guild_id))

    if not current and not queue_items:
        return "Очередь пуста."

    text = ""

    if current:
        text += (
            f"**Сейчас играет:** {current.title}\n"
            f"Добавил: {current.requested_by}\n\n"
        )

    if queue_items:
        text += "**Далее в очереди:**\n"

        for index, track in enumerate(queue_items[:10], start=1):
            text += (
                f"{index}. {track.title} "
                f"— добавил {track.requested_by}\n"
            )

        if len(queue_items) > 10:
            text += f"\nИ ещё треков: {len(queue_items) - 10}"

    return text


def build_now_playing_embed(guild_id: int) -> discord.Embed:
    track = current_tracks.get(guild_id)
    details = track_details.get(guild_id, {})
    queue_count = len(get_queue(guild_id))

    if not track:
        embed = discord.Embed(
            title="🎵 Музыкальная панель",
            description=(
                "Сейчас ничего не играет.\n\n"
                "Добавь музыку через `/search` или `/play`."
            ),
            color=discord.Color.dark_grey(),
        )

        embed.add_field(
            name="Повтор",
            value=get_repeat_text(guild_id),
            inline=True,
        )

        embed.add_field(
            name="В очереди",
            value=str(queue_count),
            inline=True,
        )

        embed.set_footer(
            text=(
                "Бот отключится через 2 минуты, "
                "если очередь будет пуста."
            )
        )

        return embed

    uploader = details.get("uploader", "Неизвестный автор")
    duration = format_duration(details.get("duration"))
    thumbnail = details.get("thumbnail")

    embed = discord.Embed(
        title="🎵 Сейчас играет",
        description=f"**{track.title}**",
        color=discord.Color.blurple(),
    )

    embed.add_field(
        name="Автор / канал",
        value=uploader,
        inline=True,
    )

    embed.add_field(
        name="Длительность",
        value=duration,
        inline=True,
    )

    embed.add_field(
        name="Добавил",
        value=track.requested_by,
        inline=True,
    )

    embed.add_field(
        name="Источник",
        value=track.source_name,
        inline=True,
    )

    embed.add_field(
        name="В очереди",
        value=str(queue_count),
        inline=True,
    )

    embed.add_field(
        name="Повтор",
        value=get_repeat_text(guild_id),
        inline=True,
    )

    if thumbnail:
        embed.set_thumbnail(url=thumbnail)

    embed.set_footer(
        text=(
            "Используй кнопки ниже. "
            "Бот отключится через 2 минуты пустой очереди."
        )
    )

    return embed


async def send_or_update_panel(guild_id: int, channel=None):
    """Удаляет прежнюю панель и отправляет актуальную последним сообщением."""
    async with get_panel_lock(guild_id):
        location = panel_locations.get(guild_id)

        if channel is None and location:
            channel_id, _ = location
            try:
                channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden):
                await delete_panel_location(guild_id)
                return None
            except discord.HTTPException as error:
                print(f"Не удалось получить канал панели: {error!r}")
                return None

        if channel is None:
            return None

        if location:
            old_channel_id, old_message_id = location
            try:
                old_channel = channel
                if old_channel_id != channel.id:
                    old_channel = bot.get_channel(old_channel_id) or await bot.fetch_channel(old_channel_id)
                old_message = await old_channel.fetch_message(old_message_id)
                await old_message.delete()
            except discord.NotFound:
                pass
            except discord.Forbidden:
                print("Нет права «Управление сообщениями» для удаления старой панели.")
                return None
            except discord.HTTPException as error:
                print(f"Не удалось удалить старую панель: {error!r}")
                return None

        try:
            message = await channel.send(
                embed=build_now_playing_embed(guild_id),
                view=MusicPanel(),
            )
            await save_panel_location(guild_id, channel.id, message.id)
            return message
        except (discord.Forbidden, discord.HTTPException) as error:
            print(f"Не удалось отправить панель: {error!r}")
            return None


def cancel_idle_disconnect(guild_id: int):
    task = idle_disconnect_tasks.pop(guild_id, None)

    if task and not task.done():
        task.cancel()


async def disconnect_if_idle(guild_id: int):
    try:
        await asyncio.sleep(IDLE_DISCONNECT_SECONDS)

        guild = bot.get_guild(guild_id)

        if guild is None:
            return

        voice_client = guild.voice_client

        if not voice_client or not voice_client.is_connected():
            return

        queue_is_empty = not get_queue(guild_id)
        music_is_stopped = (
            not voice_client.is_playing()
            and not voice_client.is_paused()
        )

        if queue_is_empty and music_is_stopped:
            current_tracks.pop(guild_id, None)
            track_details.pop(guild_id, None)

            await voice_client.disconnect()
            await send_or_update_panel(guild_id)

            print(
                f"Автоотключение: сервер {guild_id}, "
                f"простой {IDLE_DISCONNECT_SECONDS} секунд."
            )

    except asyncio.CancelledError:
        return

    finally:
        task = asyncio.current_task()

        if idle_disconnect_tasks.get(guild_id) is task:
            idle_disconnect_tasks.pop(guild_id, None)


def schedule_idle_disconnect(guild_id: int):
    cancel_idle_disconnect(guild_id)

    idle_disconnect_tasks[guild_id] = asyncio.create_task(
        disconnect_if_idle(guild_id)
    )


def is_user_in_bot_channel(
    interaction: discord.Interaction,
) -> bool:
    voice_client = interaction.guild.voice_client

    if not voice_client or not voice_client.is_connected():
        return False

    if not interaction.user.voice or not interaction.user.voice.channel:
        return False

    return interaction.user.voice.channel == voice_client.channel


async def connect_to_user_voice(
    interaction: discord.Interaction,
) -> discord.VoiceClient | None:
    if not interaction.user.voice or not interaction.user.voice.channel:
        return None

    channel = interaction.user.voice.channel
    voice_client = interaction.guild.voice_client

    if not voice_client or not voice_client.is_connected():
        voice_client = await channel.connect(
            timeout=20,
            reconnect=True,
        )
    elif voice_client.channel != channel:
        await voice_client.move_to(channel)

    return voice_client


async def on_track_finished(
    guild_id: int,
    error: Exception | None,
):
    if error:
        print(f"Ошибка воспроизведения: {error!r}")

    lock = get_lock(guild_id)

    async with lock:
        current_track = current_tracks.get(guild_id)

        if guild_id in skip_requested:
            skip_requested.discard(guild_id)

        elif current_track:
            repeat_mode = get_repeat_mode(guild_id)

            if repeat_mode == "track":
                get_queue(guild_id).appendleft(current_track)

            elif repeat_mode == "queue":
                get_queue(guild_id).append(current_track)

        current_tracks.pop(guild_id, None)
        track_details.pop(guild_id, None)

        await save_queue(guild_id)

    await start_next_track(guild_id)


async def start_next_track(guild_id: int):
    lock = get_lock(guild_id)

    async with lock:
        guild = bot.get_guild(guild_id)

        if guild is None:
            return

        voice_client = guild.voice_client

        if not voice_client or not voice_client.is_connected():
            current_tracks.pop(guild_id, None)
            track_details.pop(guild_id, None)
            return

        if voice_client.is_playing() or voice_client.is_paused():
            return

        queue = get_queue(guild_id)

        if not queue:
            current_tracks.pop(guild_id, None)
            track_details.pop(guild_id, None)

            schedule_idle_disconnect(guild_id)
            await send_or_update_panel(guild_id)

            return

        track = queue.popleft()
        current_tracks[guild_id] = track

        await save_queue(guild_id)

    try:
        info = await asyncio.to_thread(get_audio_info, track.url)

        stream_url = info["url"]
        title = info.get("title", track.title)

        current_tracks[guild_id] = Track(
            title=title,
            url=track.url,
            requested_by=track.requested_by,
            source_name=track.source_name,
            original_query=track.original_query,
        )

        track_details[guild_id] = {
            "uploader": (
                info.get("uploader")
                or info.get("channel")
                or "Неизвестный автор"
            ),
            "duration": info.get("duration"),
            "thumbnail": info.get("thumbnail"),
        }

        await add_history_item(
            guild_id,
            current_tracks[guild_id],
        )

        source = discord.PCMVolumeTransformer(
            discord.FFmpegPCMAudio(
                stream_url,
                **FFMPEG_OPTIONS,
            ),
            volume=get_volume(guild_id),
        )

        def after_playing(error):
            asyncio.run_coroutine_threadsafe(
                on_track_finished(guild_id, error),
                bot.loop,
            )

        voice_client.play(source, after=after_playing)

        await send_or_update_panel(guild_id)

    except Exception as error:
        print(f"Ошибка запуска трека: {error!r}")

        current_tracks.pop(guild_id, None)
        track_details.pop(guild_id, None)

        await send_or_update_panel(guild_id)
        await start_next_track(guild_id)


async def add_to_queue(
    interaction: discord.Interaction,
    url: str,
    title: str,
    source_name: str = "Прямая ссылка",
    original_query: str = "",
) -> tuple[int, bool]:
    voice_client = await connect_to_user_voice(interaction)

    if voice_client is None:
        raise ValueError("Сначала зайди в голосовой канал.")

    guild_id = interaction.guild.id

    cancel_idle_disconnect(guild_id)

    queue = get_queue(guild_id)

    track = Track(
        title=title,
        url=url,
        requested_by=interaction.user.display_name,
        source_name=source_name,
        original_query=original_query,
    )

    is_busy = voice_client.is_playing() or voice_client.is_paused()
    has_current = guild_id in current_tracks

    if not is_busy and not has_current and not queue:
        queue.append(track)

        await save_queue(guild_id)
        await start_next_track(guild_id)

        return 0, True

    queue.append(track)

    await save_queue(guild_id)
    await send_or_update_panel(guild_id)

    return len(queue), False


class MatchSelect(discord.ui.Select):
    def __init__(self, pending: PendingSelection, owner_id: int):
        options = []
        for index, candidate in enumerate(pending.candidates[:25], start=1):
            details = f"{candidate.source_name}"
            if candidate.duration_text:
                details += f" • {candidate.duration_text}"
            if candidate.uploader:
                details += f" • {candidate.uploader[:40]}"
            options.append(
                discord.SelectOption(
                    label=f"{index}. {candidate.title[:90]}",
                    description=details[:100],
                    value=str(index - 1),
                )
            )
        super().__init__(
            placeholder="Выбери, откуда воспроизводить",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.pending = pending
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Это меню доступно только тому, кто запустил команду.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)
        choice = self.pending.candidates[int(self.values[0])]

        try:
            position, started = await add_to_queue(
                interaction=interaction,
                url=choice.url,
                title=self.pending.original_title or choice.title,
                source_name=choice.source_name,
                original_query=self.pending.query,
            )
            for item in self.view.children:
                item.disabled = True
            await interaction.message.edit(view=self.view)

            if started:
                message = (
                    f"Сейчас играет: **{self.pending.original_title or choice.title}** "
                    f"(источник: {choice.source_name})."
                )
            else:
                message = (
                    f"Добавлено в очередь под номером **{position}**: "
                    f"**{self.pending.original_title or choice.title}** "
                    f"(источник: {choice.source_name})."
                )
            await interaction.followup.send(message)
        except Exception as error:
            print(f"Ошибка выбора источника: {error!r}")
            traceback.print_exc()
            await interaction.followup.send(
                f"Не удалось добавить выбранный вариант: {error}",
                ephemeral=True,
            )


class MatchChoiceView(discord.ui.View):
    def __init__(self, pending: PendingSelection, owner_id: int):
        super().__init__(timeout=90)
        self.add_item(MatchSelect(pending=pending, owner_id=owner_id))


class SearchButton(discord.ui.Button):
    def __init__(
        self,
        index: int,
        track: dict,
        owner_id: int,
    ):
        super().__init__(
            label=str(index + 1),
            style=discord.ButtonStyle.primary,
        )

        self.track = track
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Эти кнопки доступны только тому, "
                "кто запустил поиск.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)

        try:
            title = self.track.get("title", "Без названия")

            position, started = await add_to_queue(
                interaction=interaction,
                url=self.track["webpage_url"],
                title=title,
            )

            for item in self.view.children:
                item.disabled = True

            await interaction.message.edit(view=self.view)

            if started:
                message = f"Сейчас играет: **{title}**"
            else:
                message = (
                    f"Добавлено в очередь под номером "
                    f"**{position}**: **{title}**"
                )

            await interaction.followup.send(message)

        except ValueError as error:
            await interaction.followup.send(
                str(error),
                ephemeral=True,
            )

        except Exception as error:
            print(f"Ошибка выбора поиска: {error!r}")

            await interaction.followup.send(
                "Не удалось добавить трек. "
                "Проверь журнал сервера."
            )


class SearchView(discord.ui.View):
    def __init__(
        self,
        tracks: list[dict],
        owner_id: int,
    ):
        super().__init__(timeout=60)

        for index, track in enumerate(tracks):
            self.add_item(
                SearchButton(
                    index=index,
                    track=track,
                    owner_id=owner_id,
                )
            )


class FavoriteButton(discord.ui.Button):
    def __init__(
        self,
        index: int,
        track: Track,
        owner_id: int,
    ):
        label = f"{index + 1}. {track.title}"

        if len(label) > 80:
            label = label[:77] + "..."

        super().__init__(
            label=label,
            style=discord.ButtonStyle.secondary,
        )

        self.track = track
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Это избранное принадлежит другому пользователю.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)

        try:
            position, started = await add_to_queue(
                interaction=interaction,
                url=self.track.url,
                title=self.track.title,
                source_name=self.track.source_name,
                original_query=self.track.original_query,
            )

            if started:
                message = (
                    f"Включаю избранный трек: "
                    f"**{self.track.title}**"
                )
            else:
                message = (
                    f"Добавлено из избранного под номером "
                    f"**{position}**: **{self.track.title}**"
                )

            await interaction.followup.send(message)

        except ValueError as error:
            await interaction.followup.send(
                str(error),
                ephemeral=True,
            )

        except Exception as error:
            print(f"Ошибка запуска избранного: {error!r}")

            await interaction.followup.send(
                "Не удалось добавить трек из избранного."
            )


class FavoritesView(discord.ui.View):
    def __init__(
        self,
        tracks: list[Track],
        owner_id: int,
    ):
        super().__init__(timeout=120)

        for index, track in enumerate(tracks[:20]):
            self.add_item(
                FavoriteButton(
                    index=index,
                    track=track,
                    owner_id=owner_id,
                )
            )


class MusicPanel(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def check_voice_channel(
        self,
        interaction: discord.Interaction,
    ) -> bool:
        if not is_user_in_bot_channel(interaction):
            await interaction.response.send_message(
                "Зайди в тот же голосовой канал, "
                "в котором находится бот.",
                ephemeral=True,
            )
            return False

        return True

    @discord.ui.button(
        emoji="⏯️",
        label="Пауза / Продолжить",
        style=discord.ButtonStyle.secondary,
        custom_id="music_panel_pause_resume",
        row=0,
    )
    async def pause_resume(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if not await self.check_voice_channel(interaction):
            return

        voice_client = interaction.guild.voice_client

        if voice_client.is_paused():
            voice_client.resume()

            await interaction.response.send_message(
                "Музыка продолжается.",
                ephemeral=True,
            )
            return

        if voice_client.is_playing():
            voice_client.pause()

            await interaction.response.send_message(
                "Музыка на паузе.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            "Сейчас ничего не проигрывается.",
            ephemeral=True,
        )

    @discord.ui.button(
        emoji="⏭️",
        label="Пропустить",
        style=discord.ButtonStyle.primary,
        custom_id="music_panel_skip",
        row=0,
    )
    async def skip(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if not await self.check_voice_channel(interaction):
            return

        voice_client = interaction.guild.voice_client

        if not voice_client.is_playing() and not voice_client.is_paused():
            await interaction.response.send_message(
                "Сейчас ничего не проигрывается.",
                ephemeral=True,
            )
            return

        skip_requested.add(interaction.guild.id)
        voice_client.stop()

        await interaction.response.send_message(
            "Текущий трек пропущен.",
            ephemeral=True,
        )

    @discord.ui.button(
        emoji="⏹️",
        label="Стоп",
        style=discord.ButtonStyle.danger,
        custom_id="music_panel_stop",
        row=0,
    )
    async def stop(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if not await self.check_voice_channel(interaction):
            return

        guild_id = interaction.guild.id
        voice_client = interaction.guild.voice_client

        skip_requested.add(guild_id)
        get_queue(guild_id).clear()

        await save_queue(guild_id)

        if voice_client.is_playing() or voice_client.is_paused():
            voice_client.stop()
        else:
            current_tracks.pop(guild_id, None)
            track_details.pop(guild_id, None)

            schedule_idle_disconnect(guild_id)
            await send_or_update_panel(guild_id)

        await interaction.response.send_message(
            "Воспроизведение остановлено, очередь очищена.",
            ephemeral=True,
        )

    @discord.ui.button(
        emoji="📃",
        label="Очередь",
        style=discord.ButtonStyle.secondary,
        custom_id="music_panel_queue",
        row=0,
    )
    async def queue(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if not await self.check_voice_channel(interaction):
            return

        await interaction.response.send_message(
            get_queue_text(interaction.guild.id),
            ephemeral=True,
        )

    @discord.ui.button(
        emoji="🎵",
        label="Сейчас играет",
        style=discord.ButtonStyle.secondary,
        custom_id="music_panel_now_playing",
        row=0,
    )
    async def now_playing(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        await interaction.response.send_message(
            embed=build_now_playing_embed(interaction.guild.id),
            ephemeral=True,
        )

    @discord.ui.button(
        emoji="❤️",
        label="В избранное",
        style=discord.ButtonStyle.danger,
        custom_id="music_panel_favorite_add",
        row=1,
    )
    async def favorite_add(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if not await self.check_voice_channel(interaction):
            return

        track = current_tracks.get(interaction.guild.id)

        if not track:
            await interaction.response.send_message(
                "Сейчас нет трека, который можно добавить.",
                ephemeral=True,
            )
            return

        added = await add_favorite(
            guild_id=interaction.guild.id,
            user=interaction.user,
            track=track,
        )

        if added:
            text = f"Добавлено в избранное: **{track.title}**"
        else:
            text = "Этот трек уже есть в твоём избранном."

        await interaction.response.send_message(
            text,
            ephemeral=True,
        )

    @discord.ui.button(
        emoji="⭐",
        label="Избранное",
        style=discord.ButtonStyle.secondary,
        custom_id="music_panel_favorites",
        row=1,
    )
    async def favorites(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        tracks = await get_favorites(
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
        )

        if not tracks:
            await interaction.response.send_message(
                "Твоё избранное пока пусто. "
                "Нажми ❤️ во время проигрывания трека.",
                ephemeral=True,
            )
            return

        text = "**Твоё избранное:**\n\n"

        for index, track in enumerate(tracks, start=1):
            text += f"{index}. {track.title}\n"

        await interaction.response.send_message(
            text,
            view=FavoritesView(
                tracks=tracks,
                owner_id=interaction.user.id,
            ),
            ephemeral=True,
        )

    @discord.ui.button(
        emoji="🔀",
        label="Перемешать",
        style=discord.ButtonStyle.primary,
        custom_id="music_panel_shuffle",
        row=1,
    )
    async def shuffle(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if not await self.check_voice_channel(interaction):
            return

        guild_id = interaction.guild.id
        queue = get_queue(guild_id)

        if len(queue) < 2:
            await interaction.response.send_message(
                "Для перемешивания нужно минимум 2 трека в очереди.",
                ephemeral=True,
            )
            return

        queue_items = list(queue)
        random.shuffle(queue_items)

        queue.clear()
        queue.extend(queue_items)

        await save_queue(guild_id)
        await send_or_update_panel(guild_id)

        await interaction.response.send_message(
            "Ожидающие треки перемешаны.",
            ephemeral=True,
        )

    @discord.ui.button(
        emoji="🔁",
        label="Повтор",
        style=discord.ButtonStyle.secondary,
        custom_id="music_panel_repeat",
        row=1,
    )
    async def repeat(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if not await self.check_voice_channel(interaction):
            return

        guild_id = interaction.guild.id
        current_mode = get_repeat_mode(guild_id)

        if current_mode == "off":
            repeat_modes[guild_id] = "track"
        elif current_mode == "track":
            repeat_modes[guild_id] = "queue"
        else:
            repeat_modes[guild_id] = "off"

        await save_guild_settings(guild_id)
        await send_or_update_panel(guild_id)

        await interaction.response.send_message(
            f"Режим повтора: **{get_repeat_text(guild_id)}**",
            ephemeral=True,
        )

    @discord.ui.button(
        emoji="📜",
        label="История",
        style=discord.ButtonStyle.secondary,
        custom_id="music_panel_history",
        row=1,
    )
    async def history(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        history_items = await get_history(interaction.guild.id)

        if not history_items:
            await interaction.response.send_message(
                "История пока пуста.",
                ephemeral=True,
            )
            return

        text = "**Последние воспроизведённые треки:**\n\n"

        for index, item in enumerate(history_items, start=1):
            title, requested_by, played_at = item

            text += (
                f"{index}. {title}\n"
                f"Добавил: {requested_by}, время: {played_at}\n\n"
            )

        await interaction.response.send_message(
            text[:1900],
            ephemeral=True,
        )


intents = discord.Intents.default()


class DiscordBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)

    async def setup_hook(self):
        await init_database()
        await load_saved_data()

        self.add_view(MusicPanel())

        guild = discord.Object(id=GUILD_ID)

        self.tree.copy_global_to(guild=guild)
        synced = await self.tree.sync(guild=guild)

        print(f"Синхронизировано команд: {len(synced)}")

    async def on_ready(self):
        print(f"Бот подключён как {self.user}.")


bot = DiscordBot()


@bot.tree.command(
    name="ping",
    description="Проверить, работает ли бот",
)
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("Понг! Бот работает.")


@bot.tree.command(
    name="join",
    description="Подключить бота к твоему голосовому каналу",
)
async def join(interaction: discord.Interaction):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message(
            "Сначала зайди в голосовой канал.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True)

    try:
        guild_id = interaction.guild.id

        cancel_idle_disconnect(guild_id)

        voice_client = await connect_to_user_voice(interaction)

        await interaction.followup.send(
            f"Подключился к каналу: **{voice_client.channel.name}**"
        )

        if get_queue(guild_id):
            await start_next_track(guild_id)

        await send_or_update_panel(guild_id)

    except Exception as error:
        print(f"Ошибка /join: {error!r}")

        await interaction.followup.send(
            "Не удалось подключиться к голосовому каналу."
        )


@bot.tree.command(
    name="leave",
    description="Отключить бота от голосового канала",
)
async def leave(interaction: discord.Interaction):
    voice_client = interaction.guild.voice_client

    if not voice_client or not voice_client.is_connected():
        await interaction.response.send_message(
            "Я сейчас не подключён к голосовому каналу.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id

    cancel_idle_disconnect(guild_id)
    skip_requested.add(guild_id)

    get_queue(guild_id).clear()
    current_tracks.pop(guild_id, None)
    track_details.pop(guild_id, None)

    await save_queue(guild_id)

    if voice_client.is_playing() or voice_client.is_paused():
        voice_client.stop()

    await voice_client.disconnect()
    await send_or_update_panel(guild_id)

    await interaction.response.send_message(
        "Отключился и очистил очередь."
    )


@bot.tree.command(
    name="testsound",
    description="Проиграть тестовый звук",
)
async def testsound(interaction: discord.Interaction):
    voice_client = interaction.guild.voice_client

    if not voice_client or not voice_client.is_connected():
        await interaction.response.send_message(
            "Сначала используй /join.",
            ephemeral=True,
        )
        return

    if voice_client.is_playing() or voice_client.is_paused():
        await interaction.response.send_message(
            "Сейчас уже проигрывается звук.",
            ephemeral=True,
        )
        return

    if not os.path.exists(TEST_SOUND_PATH):
        await interaction.response.send_message(
            "Не найден файл sounds/test.mp3.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id

    cancel_idle_disconnect(guild_id)

    source = discord.PCMVolumeTransformer(
        discord.FFmpegPCMAudio(TEST_SOUND_PATH),
        volume=get_volume(guild_id),
    )

    def after_test_sound(error):
        if error:
            print(f"Ошибка тестового звука: {error!r}")

        asyncio.run_coroutine_threadsafe(
            start_next_track(guild_id),
            bot.loop,
        )

    voice_client.play(source, after=after_test_sound)

    await interaction.response.send_message(
        "Включаю тестовый звук."
    )


@bot.tree.command(
    name="play",
    description="Добавить ссылку YouTube, Spotify или Яндекс Музыки в очередь",
)
async def play(interaction: discord.Interaction, ссылка: str):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message(
            "Сначала зайди в голосовой канал.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True)

    try:
        if is_spotify_url(ссылка):
            source_label = "Spotify"
            source_type, music_tracks = await asyncio.to_thread(get_spotify_tracks, ссылка)
        elif is_yandex_music_url(ссылка):
            source_label = "Яндекс Музыка"
            source_type, music_tracks = await asyncio.to_thread(get_yandex_tracks, ссылка)
        else:
            music_tracks = None

        if music_tracks is not None:
            if not music_tracks:
                raise RuntimeError(f"В ссылке {source_label} не найдено доступных треков.")

            if source_type == "трек":
                search_query, original_title = music_tracks[0]
                print(f"/play: собираю кандидатов для {source_label}: {search_query}")
                candidates = await asyncio.to_thread(build_candidates, search_query)
                print(f"/play: найдено кандидатов: {len(candidates)}")
                if not candidates:
                    raise RuntimeError(
                        f"Не удалось найти версии трека {source_label} на YouTube или SoundCloud."
                    )

                text = f"Выбери источник для **{original_title}**\n\n"
                for index, candidate in enumerate(candidates[:10], start=1):
                    duration = f" ({candidate.duration_text})" if candidate.duration_text else ""
                    uploader = f" — {candidate.uploader}" if candidate.uploader else ""
                    text += f"**{index}.** [{candidate.source_name}] {candidate.title}{duration}{uploader}\n"

                await interaction.followup.send(
                    text[:1900],
                    view=MatchChoiceView(
                        pending=PendingSelection(
                            original_title=original_title,
                            query=search_query,
                            candidates=candidates[:25],
                        ),
                        owner_id=interaction.user.id,
                    ),
                )
                return

            added = 0
            started_title = None
            failed = 0
            for search_query, original_title in music_tracks:
                try:
                    print(f"/play: автопоиск для {source_label}: {search_query}")
                    url, found_title, source_name = await asyncio.to_thread(resolve_music_track, search_query)
                    print(f"/play: выбран источник {source_name}: {url}")
                    _, started = await add_to_queue(
                        interaction=interaction,
                        url=url,
                        title=original_title or found_title,
                        source_name=source_name,
                        original_query=search_query,
                    )
                    added += 1
                    if started:
                        started_title = original_title or found_title
                except Exception as track_error:
                    failed += 1
                    print(f"Не удалось сопоставить трек {original_title!r} из {source_label}: {track_error!r}")
                    traceback.print_exc()

            if not added:
                raise RuntimeError(
                    f"Не удалось найти версии треков {source_label} на YouTube или SoundCloud."
                )

            message = f"Добавлено из {source_label} ({source_type}): **{added}** трек(ов)."
            if started_title:
                message += f" Сейчас играет: **{started_title}**."
            if failed:
                message += f" Не найдено: **{failed}**."
            await interaction.followup.send(message)
            return

        info = await asyncio.to_thread(get_audio_info, ссылка)
        title = info.get("title", "Без названия")
        url = info.get("webpage_url", ссылка)
        position, started = await add_to_queue(
            interaction=interaction,
            url=url,
            title=title,
        )
        if started:
            message = f"Сейчас играет: **{title}**"
        else:
            message = f"Добавлено в очередь под номером **{position}**: **{title}**"
        await interaction.followup.send(message)

    except Exception as error:
        print(f"Ошибка /play: {error!r}")
        traceback.print_exc()

        await interaction.followup.send(
            f"Не удалось добавить аудио: {error}"
        )


@bot.tree.command(
    name="search",
    description="Найти трек по названию",
)
async def search(interaction: discord.Interaction, запрос: str):
    await interaction.response.defer(thinking=True)

    try:
        tracks = await asyncio.to_thread(search_videos, запрос)

        if not tracks:
            await interaction.followup.send(
                "Ничего не найдено. Попробуй другой запрос."
            )
            return

        text = f"Результаты для: **{запрос}**\n\n"

        for index, track in enumerate(tracks, start=1):
            title = track.get("title", "Без названия")
            uploader = track.get("uploader", "Неизвестный автор")
            duration = track.get("duration_string", "")
            duration_text = f" ({duration})" if duration else ""

            text += (
                f"**{index}.** {title}\n"
                f"Автор: {uploader}{duration_text}\n\n"
            )

        await interaction.followup.send(
            text,
            view=SearchView(
                tracks=tracks,
                owner_id=interaction.user.id,
            ),
        )

    except Exception as error:
        print(f"Ошибка /search: {error!r}")

        await interaction.followup.send(
            "Не удалось выполнить поиск. "
            "Проверь журнал сервера."
        )


@bot.tree.command(
    name="queue",
    description="Показать текущий трек и очередь",
)
async def queue(interaction: discord.Interaction):
    await interaction.response.send_message(
        get_queue_text(interaction.guild.id)
    )


@bot.tree.command(
    name="skip",
    description="Пропустить текущий трек",
)
async def skip(interaction: discord.Interaction):
    voice_client = interaction.guild.voice_client

    if not is_user_in_bot_channel(interaction):
        await interaction.response.send_message(
            "Зайди в тот же голосовой канал, "
            "в котором находится бот.",
            ephemeral=True,
        )
        return

    if not voice_client.is_playing() and not voice_client.is_paused():
        await interaction.response.send_message(
            "Сейчас ничего не проигрывается.",
            ephemeral=True,
        )
        return

    skip_requested.add(interaction.guild.id)
    voice_client.stop()

    await interaction.response.send_message(
        "Текущий трек пропущен."
    )


@bot.tree.command(
    name="clear",
    description="Очистить ожидающие треки",
)
async def clear(interaction: discord.Interaction):
    if not is_user_in_bot_channel(interaction):
        await interaction.response.send_message(
            "Зайди в тот же голосовой канал, "
            "в котором находится бот.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id
    queue_items = get_queue(guild_id)
    count = len(queue_items)

    queue_items.clear()

    await save_queue(guild_id)
    await send_or_update_panel(guild_id)

    await interaction.response.send_message(
        f"Очередь очищена. Удалено треков: {count}."
    )


@bot.tree.command(
    name="stop",
    description="Остановить музыку и очистить очередь",
)
async def stop(interaction: discord.Interaction):
    if not is_user_in_bot_channel(interaction):
        await interaction.response.send_message(
            "Зайди в тот же голосовой канал, "
            "в котором находится бот.",
            ephemeral=True,
        )
        return

    voice_client = interaction.guild.voice_client
    guild_id = interaction.guild.id

    skip_requested.add(guild_id)
    get_queue(guild_id).clear()

    await save_queue(guild_id)

    if voice_client.is_playing() or voice_client.is_paused():
        voice_client.stop()
    else:
        current_tracks.pop(guild_id, None)
        track_details.pop(guild_id, None)

        schedule_idle_disconnect(guild_id)
        await send_or_update_panel(guild_id)

    await interaction.response.send_message(
        "Воспроизведение остановлено, очередь очищена."
    )


@bot.tree.command(
    name="pause",
    description="Поставить музыку на паузу",
)
async def pause(interaction: discord.Interaction):
    voice_client = interaction.guild.voice_client

    if not is_user_in_bot_channel(interaction):
        await interaction.response.send_message(
            "Зайди в тот же голосовой канал, "
            "в котором находится бот.",
            ephemeral=True,
        )
        return

    if voice_client.is_paused():
        await interaction.response.send_message(
            "Музыка уже стоит на паузе.",
            ephemeral=True,
        )
        return

    if not voice_client.is_playing():
        await interaction.response.send_message(
            "Сейчас ничего не проигрывается.",
            ephemeral=True,
        )
        return

    voice_client.pause()

    await interaction.response.send_message(
        "Музыка на паузе."
    )


@bot.tree.command(
    name="resume",
    description="Продолжить музыку после паузы",
)
async def resume(interaction: discord.Interaction):
    voice_client = interaction.guild.voice_client

    if not is_user_in_bot_channel(interaction):
        await interaction.response.send_message(
            "Зайди в тот же голосовой канал, "
            "в котором находится бот.",
            ephemeral=True,
        )
        return

    if not voice_client.is_paused():
        await interaction.response.send_message(
            "Музыка сейчас не стоит на паузе.",
            ephemeral=True,
        )
        return

    voice_client.resume()

    await interaction.response.send_message(
        "Музыка продолжается."
    )


@bot.tree.command(
    name="nowplaying",
    description="Показать текущий трек",
)
async def nowplaying(interaction: discord.Interaction):
    await interaction.response.send_message(
        embed=build_now_playing_embed(interaction.guild.id)
    )


@bot.tree.command(
    name="volume",
    description="Установить громкость от 0 до 100",
)
async def volume(interaction: discord.Interaction, процент: int):
    if процент < 0 or процент > 100:
        await interaction.response.send_message(
            "Укажи значение от 0 до 100.",
            ephemeral=True,
        )
        return

    if not is_user_in_bot_channel(interaction):
        await interaction.response.send_message(
            "Зайди в тот же голосовой канал, "
            "в котором находится бот.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id
    new_volume = процент / 100

    volumes[guild_id] = new_volume

    await save_guild_settings(guild_id)

    voice_client = interaction.guild.voice_client

    if isinstance(
        voice_client.source,
        discord.PCMVolumeTransformer,
    ):
        voice_client.source.volume = new_volume

    await interaction.response.send_message(
        f"Громкость установлена: **{процент}%**."
    )


@bot.tree.command(
    name="favorites",
    description="Показать твоё избранное",
)
async def favorites(interaction: discord.Interaction):
    tracks = await get_favorites(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
    )

    if not tracks:
        await interaction.response.send_message(
            "Твоё избранное пока пусто. "
            "Добавляй треки кнопкой ❤️ на панели.",
            ephemeral=True,
        )
        return

    text = "**Твоё избранное:**\n\n"

    for index, track in enumerate(tracks, start=1):
        text += f"{index}. {track.title}\n"

    await interaction.response.send_message(
        text,
        view=FavoritesView(
            tracks=tracks,
            owner_id=interaction.user.id,
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="history",
    description="Показать последние 20 треков",
)
async def history(interaction: discord.Interaction):
    history_items = await get_history(interaction.guild.id)

    if not history_items:
        await interaction.response.send_message(
            "История пока пуста."
        )
        return

    text = "**Последние воспроизведённые треки:**\n\n"

    for index, item in enumerate(history_items, start=1):
        title, requested_by, played_at = item

        text += (
            f"{index}. {title}\n"
            f"Добавил: {requested_by}, время: {played_at}\n\n"
        )

    await interaction.response.send_message(text[:1900])


@bot.tree.command(
    name="shuffle",
    description="Перемешать ожидающие треки",
)
async def shuffle(interaction: discord.Interaction):
    if not is_user_in_bot_channel(interaction):
        await interaction.response.send_message(
            "Зайди в тот же голосовой канал, "
            "в котором находится бот.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id
    queue_items = get_queue(guild_id)

    if len(queue_items) < 2:
        await interaction.response.send_message(
            "Для перемешивания нужно минимум 2 трека в очереди.",
            ephemeral=True,
        )
        return

    tracks = list(queue_items)
    random.shuffle(tracks)

    queue_items.clear()
    queue_items.extend(tracks)

    await save_queue(guild_id)
    await send_or_update_panel(guild_id)

    await interaction.response.send_message(
        "Ожидающие треки перемешаны."
    )


@bot.tree.command(
    name="repeat",
    description="Сменить режим повтора",
)
async def repeat(interaction: discord.Interaction):
    if not is_user_in_bot_channel(interaction):
        await interaction.response.send_message(
            "Зайди в тот же голосовой канал, "
            "в котором находится бот.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id
    current_mode = get_repeat_mode(guild_id)

    if current_mode == "off":
        repeat_modes[guild_id] = "track"
    elif current_mode == "track":
        repeat_modes[guild_id] = "queue"
    else:
        repeat_modes[guild_id] = "off"

    await save_guild_settings(guild_id)
    await send_or_update_panel(guild_id)

    await interaction.response.send_message(
        f"Режим повтора: **{get_repeat_text(guild_id)}**"
    )


@bot.tree.command(
    name="panel",
    description="Создать музыкальную панель внизу канала",
)
async def panel(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "Команда доступна только администраторам сервера.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)
    message = await send_or_update_panel(interaction.guild.id, interaction.channel)

    if message is None:
        await interaction.followup.send(
            "Не удалось создать панель. Проверь права бота на удаление и отправку сообщений.",
            ephemeral=True,
        )
        return

    await interaction.followup.send(
        "Панель перемещена в самый низ канала.",
        ephemeral=True,
    )

bot.run(TOKEN)