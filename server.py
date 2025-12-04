import os
import json
import sqlite3
import uuid
import datetime
import smtplib
import ssl
import io
import zipfile
import shutil
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, jsonify, send_from_directory, request, Response, g, send_file
from werkzeug.utils import secure_filename
from flask_cors import CORS 
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.mp4 import MP4
from mutagen.flac import FLAC
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import random 
import sys
import re 
from functools import wraps

# --- Configuration ---
MUSIC_DIR = os.path.join(os.getcwd(), 'library')
DATABASE = 'audio_vault.db'
DEFAULT_SONG_LIMIT = 1000 
CLIENT_HTML_FILENAME = 'client.html'
DOWNLOADER_HTML_FILENAME = 'downloader.html'
ALLOWED_EXTENSIONS = {'mp3', 'm4a', 'flac', 'ogg'}

os.makedirs(MUSIC_DIR, exist_ok=True)
PORT = 5000

# --- Email Config ---
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = "jasonholtrey" 
SENDER_PASSWORD = "vrkyhgvkptijxghk" 

# --- Global State for Downloads ---
# key: playlist_id, value: {'status': str, 'current': int, 'total': int}
download_status = {}

app = Flask(__name__)
CORS(app) 

# --- Database Functions ---
def get_db_conn():
    """Returns a new database connection with retry logic for locking."""
    if 'db_conn' not in g:
        retries = 5
        while retries > 0:
            try:
                g.db_conn = sqlite3.connect(DATABASE, timeout=30)
                g.db_conn.row_factory = sqlite3.Row
                g.db_conn.execute("PRAGMA journal_mode=WAL")
                return g.db_conn
            except sqlite3.OperationalError as e:
                if "locked" in str(e):
                    time.sleep(0.2)
                    retries -= 1
                else:
                    raise e
        g.db_conn = sqlite3.connect(DATABASE, timeout=30)
        g.db_conn.row_factory = sqlite3.Row
        g.db_conn.execute("PRAGMA journal_mode=WAL")
        
    return g.db_conn

@app.teardown_appcontext
def close_db_conn(exception):
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()

def init_db():
    print("--- Checking Database Schema ---")
    try:
        conn = sqlite3.connect(DATABASE, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        c = conn.cursor()
        
        c.execute("""
            CREATE TABLE IF NOT EXISTS music (
                id TEXT PRIMARY KEY, filepath TEXT UNIQUE, title TEXT, artist TEXT, album TEXT, 
                duration INTEGER, last_modified REAL, uploaded_by TEXT, play_count INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                email TEXT PRIMARY KEY, login_code TEXT, code_expiry REAL, last_login TEXT,
                current_song_id TEXT, last_active REAL, is_admin INTEGER DEFAULT 0,
                display_name TEXT,
                current_song_progress REAL DEFAULT 0,
                current_view_type TEXT,
                current_view_id TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                token TEXT PRIMARY KEY, user_email TEXT, created_at TEXT, expires_at REAL,
                FOREIGN KEY (user_email) REFERENCES users(email) ON DELETE CASCADE
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS playlists (
                id TEXT PRIMARY KEY, user_email TEXT, name TEXT, is_public INTEGER DEFAULT 0, created_at TEXT,
                FOREIGN KEY (user_email) REFERENCES users(email) ON DELETE CASCADE
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS playlist_songs (
                playlist_id TEXT, song_id TEXT,
                PRIMARY KEY (playlist_id, song_id),
                FOREIGN KEY (playlist_id) REFERENCES playlists(id) ON DELETE CASCADE,
                FOREIGN KEY (song_id) REFERENCES music(id)
            )
        """)
        
        def ensure_column(table, column, definition):
            try:
                c.execute(f"SELECT {column} FROM {table} LIMIT 1")
            except sqlite3.OperationalError:
                print(f"[DB MIGRATION] Adding missing column '{column}' to '{table}'...")
                try: c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                except Exception as e: print(f"[DB ERROR] Could not add column {column}: {e}")
        
        ensure_column('playlists', 'is_public', 'INTEGER DEFAULT 0')
        ensure_column('playlists', 'created_at', 'TEXT')
        ensure_column('music', 'uploaded_by', 'TEXT')
        ensure_column('music', 'play_count', 'INTEGER DEFAULT 0')
        ensure_column('users', 'current_song_id', 'TEXT')
        ensure_column('users', 'last_active', 'REAL')
        ensure_column('users', 'is_admin', 'INTEGER DEFAULT 0')
        ensure_column('users', 'display_name', 'TEXT')
        # New State Tracking Columns
        ensure_column('users', 'current_song_progress', 'REAL DEFAULT 0')
        ensure_column('users', 'current_view_type', 'TEXT')
        ensure_column('users', 'current_view_id', 'TEXT')

        # --- ADMIN SEED ---
        conn.execute("UPDATE users SET is_admin = 1 WHERE email = 'jasonholtrey@gmail.com'")
        
        conn.commit()
        conn.close()
        print("--- Database Schema Check Complete ---")
    except Exception as e:
        print(f"[CRITICAL DB ERROR] {e}")

# --- Utility Functions ---
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def parse_metadata(filepath):
    metadata = {}
    try:
        if filepath.lower().endswith('.mp3'): audio = MP3(filepath, ID3=EasyID3)
        elif filepath.lower().endswith('.m4a'): audio = MP4(filepath)
        elif filepath.lower().endswith('.flac'): audio = FLAC(filepath)
        else: return None
        
        filename = os.path.basename(filepath)
        if len(filename) > 33 and filename[32] == '_':
             clean_name = filename[33:]
        else:
             clean_name = filename
             
        fallback_title = os.path.splitext(clean_name)[0]

        metadata['title'] = audio.get('title', [fallback_title])[0]
        metadata['artist'] = audio.get('artist', ['Unknown Artist'])[0]
        metadata['album'] = audio.get('album', ['Unknown Album'])[0]
        metadata['duration'] = int(audio.info.length) if hasattr(audio.info, 'length') else 0
        return metadata
    except Exception as e:
        # print(f"Error reading metadata for {filepath}: {e}") # Suppress noise
        return None

def check_duplicate(title, artist, db_conn):
    if not title or not artist: return None
    if artist.strip().lower() == 'unknown artist': return None
    cursor = db_conn.cursor()
    cursor.execute("SELECT id FROM music WHERE title = ? AND artist = ? COLLATE NOCASE", (title, artist))
    row = cursor.fetchone()
    return row['id'] if row else None

def sync_music_library():
    print("Starting music library synchronization (Background)...")
    try:
        db = sqlite3.connect(DATABASE, timeout=30) 
        db.execute("PRAGMA journal_mode=WAL")
        db.row_factory = sqlite3.Row
        cursor = db.cursor()
        current_files = set()
        
        for root, _, files in os.walk(MUSIC_DIR):
            for filename in files:
                if allowed_file(filename):
                    full_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(full_path, MUSIC_DIR)
                    current_files.add(relative_path)
                    stat = os.stat(full_path)
                    last_modified = stat.st_mtime
                    try:
                        cursor.execute("SELECT last_modified FROM music WHERE filepath=?", (relative_path,))
                        row = cursor.fetchone()
                        if row is None or row['last_modified'] != last_modified:
                            metadata = parse_metadata(full_path)
                            if metadata:
                                song_id = str(uuid.uuid5(uuid.NAMESPACE_URL, relative_path))
                                cursor.execute("""
                                    INSERT OR REPLACE INTO music (id, filepath, title, artist, album, duration, last_modified, play_count, uploaded_by) 
                                    VALUES (?, ?, ?, ?, ?, ?, ?, 
                                    COALESCE((SELECT play_count FROM music WHERE id=?), 0),
                                    COALESCE((SELECT uploaded_by FROM music WHERE id=?), 'System'))
                                """, (song_id, relative_path, metadata['title'], metadata['artist'], metadata['album'], metadata['duration'], last_modified, song_id, song_id))
                                print(f"Synced: {relative_path}")
                    except sqlite3.OperationalError: pass 
        try:
            cursor.execute("SELECT filepath FROM music")
            known_files = set([row['filepath'] for row in cursor.fetchall()])
            deleted_files = known_files - current_files
            for relative_path in deleted_files:
                cursor.execute("DELETE FROM music WHERE filepath=?", (relative_path,))
                print(f"Deleted: {relative_path}")
        except: pass

        db.commit()
        db.close()
        print("Music library sync complete.")
    except Exception as e:
        print(f"[SYNC ERROR] {e}")

class MusicFileHandler(FileSystemEventHandler):
    def __init__(self, app_context):
        self.app_context = app_context
        self.debounce_timer = None
    def on_any_event(self, event):
        if event.is_directory: return
        if self.debounce_timer and self.debounce_timer.is_alive(): self.debounce_timer.cancel()
        self.debounce_timer = threading.Timer(1.0, lambda: threading.Thread(target=sync_music_library).start())
        self.debounce_timer.start()

def start_file_monitoring(app):
    observer = Observer()
    handler = MusicFileHandler(app.app_context())
    observer.schedule(handler, MUSIC_DIR, recursive=True)
    observer.start()
    return observer

# --- Auth Decorator ---
def auth_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '') or request.args.get('token')
        if not token: return jsonify({'error': 'Token missing.'}), 401
        db = get_db_conn()
        cursor = db.cursor()
        
        cursor.execute("""
            SELECT s.user_email, u.is_admin 
            FROM user_sessions s
            JOIN users u ON s.user_email = u.email
            WHERE s.token=?
        """, (token,))
        
        row = cursor.fetchone()
        if row is None: return jsonify({'error': 'Invalid token'}), 401
        g.user_email = row['user_email']
        g.is_admin = bool(row['is_admin'])
        return f(*args, **kwargs)
    return decorated_function

def send_login_email(recipient_email, code, server_url):
    msg = MIMEMultipart("alternative")
    msg['Subject'] = "Shmotify Login Code" 
    msg['From'] = SENDER_EMAIL
    msg['To'] = recipient_email
    html = f"<html><body><h2>Welcome to Shmotify</h2><p>Your login code is: <b style='font-size: 20px;'>{code}</b></p><p>This code expires in 5 minutes.</p><p>Server URL: {server_url}</p></body></html>"
    msg.attach(MIMEText(html, "html"))
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipient_email, msg.as_string())
        return True
    except Exception as e:
        print(f"[ERROR] Email failed: {e}")
        return False

# --- Helpers for Downloader ---
def process_imported_file(filepath, playlist_id, user_email, db_conn):
    """Moves file to library and updates DB. Returns True if successful."""
    try:
        # 1. Parse Metadata
        metadata = parse_metadata(filepath)
        if not metadata: return False

        # 2. Check Duplicates
        dup_id = check_duplicate(metadata['title'], metadata['artist'], db_conn)
        
        if dup_id:
            # It's a duplicate, just link it
            db_conn.execute("INSERT OR IGNORE INTO playlist_songs (playlist_id, song_id) VALUES (?, ?)", (playlist_id, dup_id))
            db_conn.commit()
            print(f"[IMPORT] Linked duplicate: {metadata['title']}")
            try: os.remove(filepath)
            except: pass
            return True
        else:
            # 3. New Song - Move it
            filename = os.path.basename(filepath)
            base, ext = os.path.splitext(filename)
            safe_base = secure_filename(base)[:150] 
            safe_filename = f"{safe_base}{ext}"
            
            unique_name = f"{uuid.uuid4().hex}_{safe_filename}"
            dest_path = os.path.join(MUSIC_DIR, unique_name)
            
            shutil.move(filepath, dest_path)
            
            # 4. Insert into DB
            song_id = str(uuid.uuid5(uuid.NAMESPACE_URL, unique_name))
            stat = os.stat(dest_path)
            
            db_conn.execute("""
                INSERT OR REPLACE INTO music (id, filepath, title, artist, album, duration, last_modified, uploaded_by, play_count) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """, (song_id, unique_name, metadata['title'], metadata['artist'], metadata['album'], metadata['duration'], stat.st_mtime, user_email))
            
            db_conn.execute("INSERT OR IGNORE INTO playlist_songs (playlist_id, song_id) VALUES (?, ?)", (playlist_id, song_id))
            db_conn.commit()
            print(f"[IMPORT] Added new song: {metadata['title']}")
            return True
    except Exception as e:
        print(f"[IMPORT FAILED] {filepath}: {e}")
        return False

def stdout_reader(process, playlist_id):
    """Thread function to read stdout and update Total count."""
    re_found = re.compile(r"Found (\d+) songs", re.IGNORECASE)
    
    for line in iter(process.stdout.readline, ''):
        line = line.strip()
        if line:
            print(f"[SPOTDL] {line}", flush=True) # Send to console
            
            # Update Total if found
            m = re_found.search(line)
            if m:
                total = int(m.group(1))
                download_status[playlist_id]['total'] = total

# --- Background Downloader ---
def run_spotdl_download(playlist_id, playlist_url, playlist_name, user_email):
    temp_dir = os.path.join(os.getcwd(), f"temp_dl_{uuid.uuid4().hex}")
    os.makedirs(temp_dir, exist_ok=True)
    
    download_status[playlist_id] = {'status': 'Initializing', 'current': 0, 'total': '?'}
    print(f"[DOWNLOADER] Starting for {playlist_name} ({playlist_id})")
    
    try:
        # Use Popen to allow parallel processing
        cmd = ["spotdl", playlist_url, "--output", temp_dir, "--format", "mp3"]
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            cwd=temp_dir, 
            text=True, 
            bufsize=1,
            universal_newlines=True
        )

        # Start thread to log output and catch Total count
        reader_thread = threading.Thread(target=stdout_reader, args=(process, playlist_id))
        reader_thread.start()

        # Main Loop: Monitor directory for completed files while SpotDL runs
        imported_count = 0
        
        while process.poll() is None:
            time.sleep(1.0) # Poll frequency
            
            # Scan directory for new files
            for filename in os.listdir(temp_dir):
                if allowed_file(filename):
                    filepath = os.path.join(temp_dir, filename)
                    
                    # STABILITY CHECK:
                    # Check if file has been modified in the last 5 seconds.
                    # If it has, SpotDL is likely still writing audio or tags.
                    # This prevents "MetadataError" (stealing the file before tagging).
                    try:
                        mtime = os.path.getmtime(filepath)
                        if time.time() - mtime < 5: 
                            continue # File is too "hot", wait
                        
                        if os.path.getsize(filepath) > 0:
                            db = sqlite3.connect(DATABASE, timeout=10)
                            db.execute("PRAGMA journal_mode=WAL")
                            db.row_factory = sqlite3.Row  # <--- Added row_factory
                            
                            if process_imported_file(filepath, playlist_id, user_email, db):
                                imported_count += 1
                                download_status[playlist_id]['current'] = imported_count
                                download_status[playlist_id]['status'] = 'Downloading'
                            
                            db.close()
                    except OSError:
                        pass # File might have vanished (moved)

        # Process has finished. Wait for reader thread.
        reader_thread.join()
        
        # Final Sweep (Process any stragglers left in the folder)
        db = sqlite3.connect(DATABASE, timeout=30)
        db.execute("PRAGMA journal_mode=WAL")
        db.row_factory = sqlite3.Row  # <--- Added row_factory
        
        for filename in os.listdir(temp_dir):
            if allowed_file(filename):
                filepath = os.path.join(temp_dir, filename)
                # No stability check needed here, process is dead
                if process_imported_file(filepath, playlist_id, user_email, db):
                    imported_count += 1
        db.close()

        if process.returncode != 0:
            print("[DOWNLOADER] SpotDL finished with errors (check logs).")
        else:
            print(f"[DOWNLOADER] Finished {playlist_name}. Total imported: {imported_count}")
        
    except Exception as e:
        print(f"[DOWNLOADER ERROR] {e}")
    finally:
        if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
        if playlist_id in download_status:
            del download_status[playlist_id]

# --- Routes ---

@app.route('/api/download_external', methods=['POST'])
@auth_required
def download_external_playlist():
    data = request.get_json()
    url = data.get('url', '').strip()
    name = data.get('name', '').strip()
    
    if not url or not name:
        return jsonify({'error': 'URL and Playlist Name are required'}), 400
    
    # Create Playlist Immediately
    playlist_id = str(uuid.uuid4())
    created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db = get_db_conn()
    db.execute("INSERT INTO playlists (id, user_email, name, is_public, created_at) VALUES (?, ?, ?, 0, ?)", 
               (playlist_id, g.user_email, name, created_at))
    db.commit()
    
    # Initialize Status
    download_status[playlist_id] = {'status': 'Queued', 'current': 0, 'total': '?'}

    # Start Background Task
    threading.Thread(target=run_spotdl_download, args=(playlist_id, url, name, g.user_email)).start()
    
    return jsonify({'message': 'Download started!', 'playlist_id': playlist_id}), 200

# [Standard Auth Routes]
@app.route('/api/login/request', methods=['POST'])
def request_login_code():
    data = request.get_json()
    email = data.get('email', '').strip().lower()
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email): return jsonify({'error': 'Invalid email'}), 400
    code = ''.join([str(random.randint(0, 9)) for _ in range(6)])
    expiry = time.time() + 300 
    get_db_conn().execute("INSERT INTO users (email, login_code, code_expiry) VALUES (?, ?, ?) ON CONFLICT(email) DO UPDATE SET login_code=excluded.login_code, code_expiry=excluded.code_expiry", (email, code, expiry))
    get_db_conn().commit()
    server_url = f'http://127.0.0.1:{PORT}'
    threading.Thread(target=send_login_email, args=(email, code, server_url)).start()
    return jsonify({'message': 'Code sent.'}), 200

@app.route('/api/login/verify', methods=['POST'])
def verify_login_code():
    data = request.get_json()
    email = data.get('email', '').strip().lower()
    code = data.get('code', '').strip()
    db = get_db_conn()
    
    row = db.execute("""
        SELECT code_expiry, login_code, is_admin, display_name, 
               current_song_id, current_song_progress, current_view_type, current_view_id 
        FROM users WHERE email=?
    """, (email,)).fetchone()
    
    if not row or row['login_code'] != code or row['code_expiry'] < time.time():
        return jsonify({'error': 'Invalid code'}), 401
    token = str(uuid.uuid4())
    created_at = datetime.datetime.now().isoformat()
    expires_at = time.time() + (60 * 60 * 24 * 7) 
    db.execute("INSERT INTO user_sessions (token, user_email, created_at, expires_at) VALUES (?, ?, ?, ?)", (token, email, created_at, expires_at))
    db.execute("UPDATE users SET last_login = ?, login_code = NULL WHERE email = ?", (created_at, email))
    db.commit()
    
    # Return last known state
    last_state = {
        'song_id': row['current_song_id'],
        'progress': row['current_song_progress'],
        'view_type': row['current_view_type'],
        'view_id': row['current_view_id']
    }
    
    return jsonify({
        'token': token, 
        'user_id': email, 
        'is_admin': bool(row['is_admin']), 
        'display_name': row['display_name'],
        'last_state': last_state
    }), 200

@app.route('/api/user/me', methods=['GET'])
@auth_required
def get_current_user():
    """Returns current user profile and last known state."""
    db = get_db_conn()
    row = db.execute("""
        SELECT is_admin, display_name, 
               current_song_id, current_song_progress, current_view_type, current_view_id 
        FROM users WHERE email=?
    """, (g.user_email,)).fetchone()
    
    if not row: return jsonify({'error': 'User not found'}), 404
    
    last_state = {
        'song_id': row['current_song_id'],
        'progress': row['current_song_progress'],
        'view_type': row['current_view_type'],
        'view_id': row['current_view_id']
    }
    
    return jsonify({
        'user_id': g.user_email, 
        'is_admin': bool(row['is_admin']), 
        'display_name': row['display_name'],
        'last_state': last_state
    }), 200

# [Settings Routes]
@app.route('/api/user/settings', methods=['PUT'])
@auth_required
def update_settings():
    data = request.get_json()
    new_name = data.get('display_name', '').strip()
    if not new_name:
        return jsonify({'error': 'Display name cannot be empty'}), 400
    
    get_db_conn().execute("UPDATE users SET display_name=? WHERE email=?", (new_name, g.user_email))
    get_db_conn().commit()
    return jsonify({'message': 'Settings updated', 'display_name': new_name}), 200

# [Heartbeat & Activity]
@app.route('/api/heartbeat', methods=['POST'])
@auth_required
def heartbeat():
    data = request.get_json(silent=True) or {}
    db = get_db_conn()
    
    # 1. Update Current User State
    db.execute("""
        UPDATE users 
        SET current_song_id=?, current_song_progress=?, current_view_type=?, current_view_id=?, last_active=? 
        WHERE email=?
    """, (
        data.get('song_id'), 
        data.get('progress', 0), 
        data.get('view_type'), 
        data.get('view_id'), 
        time.time(), 
        g.user_email
    ))
    db.commit()

    # 2. Get Activities (Users)
    c = db.cursor()
    c.execute("""
        SELECT u.email, u.display_name, m.title, m.artist 
        FROM users u 
        LEFT JOIN music m ON u.current_song_id = m.id 
        WHERE u.last_active > ? AND u.email != ?
    """, (time.time()-120, g.user_email))
    
    activities = []
    for r in c.fetchall():
        name = r['display_name'] if r['display_name'] else r['email'].split('@')[0]
        status = f"Listening to {r['title']}" if r['title'] else "Sitting in silence"
        activities.append({'name': name, 'status': status})

    # 3. Get Public Playlists (Combined query for sidebar)
    c.execute("""
        SELECT p.id, p.name, p.user_email, u.display_name, p.created_at, COUNT(ps.song_id) as song_count 
        FROM playlists p 
        LEFT JOIN playlist_songs ps ON p.id = ps.playlist_id 
        LEFT JOIN users u ON p.user_email = u.email
        WHERE p.is_public=1 
        GROUP BY p.id, p.name, p.user_email, u.display_name, p.created_at 
        ORDER BY p.created_at DESC
    """)
    public_playlists = [dict(row) for row in c.fetchall()]

    # 4. Get Download Statuses
    active_downloads = {pid: stat for pid, stat in download_status.items()}

    return jsonify({
        'activities': activities,
        'public_playlists': public_playlists,
        'downloads': active_downloads
    }), 200

# [Song Routes]
@app.route('/api/songs', methods=['GET'])
@auth_required
def list_songs():
    limit = request.args.get('limit', DEFAULT_SONG_LIMIT, type=int)
    offset = request.args.get('offset', 0, type=int)
    c = get_db_conn().cursor()
    c.execute("SELECT COUNT(*) FROM music")
    total = c.fetchone()[0]
    c.execute("SELECT id, title, artist, album, duration, uploaded_by, play_count FROM music ORDER BY artist, album, title LIMIT ? OFFSET ?", (limit, offset))
    return jsonify({'songs': [dict(row) for row in c.fetchall()], 'total': total}), 200

@app.route('/api/stream/<song_id>', methods=['GET'])
@auth_required
def stream_song(song_id):
    row = get_db_conn().execute("SELECT filepath FROM music WHERE id=?", (song_id,)).fetchone()
    if not row: return jsonify({'error': 'Not found'}), 404
    return send_from_directory(os.path.dirname(os.path.join(MUSIC_DIR, row['filepath'])), os.path.basename(row['filepath']))

@app.route('/api/download/<song_id>', methods=['GET'])
@auth_required
def download_song(song_id):
    row = get_db_conn().execute("SELECT filepath, title FROM music WHERE id=?", (song_id,)).fetchone()
    if not row: return jsonify({'error': 'Not found'}), 404
    return send_from_directory(os.path.dirname(os.path.join(MUSIC_DIR, row['filepath'])), os.path.basename(row['filepath']), as_attachment=True)

@app.route('/api/songs/bulk_delete', methods=['POST'])
@auth_required
def bulk_delete_songs():
    data = request.get_json()
    song_ids = data.get('song_ids', [])
    if not song_ids: return jsonify({'error': 'No songs selected'}), 400
    db = get_db_conn()
    count = 0
    for song_id in song_ids:
        row = db.execute("SELECT filepath, uploaded_by FROM music WHERE id=?", (song_id,)).fetchone()
        
        # PERMISSION CHECK: Owner OR Admin
        if row and (row['uploaded_by'] == g.user_email or g.is_admin):
            full_path = os.path.join(MUSIC_DIR, row['filepath'])
            if os.path.exists(full_path):
                try: os.remove(full_path)
                except: pass
            db.execute("DELETE FROM music WHERE id=?", (song_id,))
            db.execute("DELETE FROM playlist_songs WHERE song_id=?", (song_id,))
            count += 1
    db.commit()
    return jsonify({'message': f'Deleted {count} songs.'}), 200

@app.route('/api/play_count', methods=['POST'])
@auth_required
def increment_play_count():
    get_db_conn().execute("UPDATE music SET play_count = play_count + 1 WHERE id=?", (request.get_json().get('song_id'),))
    get_db_conn().commit()
    return jsonify({'message': 'Incremented'}), 200

@app.route('/api/upload', methods=['POST'])
@auth_required
def upload_music():
    if 'file' not in request.files: return jsonify({'error': 'No file'}), 400
    file = request.files['file']
    if not file or not allowed_file(file.filename): return jsonify({'error': 'Invalid'}), 400
    unique_name = f"{uuid.uuid4().hex}_{secure_filename(file.filename)}"
    path = os.path.join(MUSIC_DIR, unique_name)
    file.save(path)
    meta = parse_metadata(path)
    if meta:
        dup_id = check_duplicate(meta['title'], meta['artist'], get_db_conn())
        if dup_id:
            os.remove(path)
            return jsonify({'error': 'Duplicate song detected (Title + Artist match).', 'code': 'DUPLICATE'}), 409
        sid = str(uuid.uuid5(uuid.NAMESPACE_URL, unique_name))
        get_db_conn().execute("INSERT OR REPLACE INTO music (id, filepath, title, artist, album, duration, last_modified, uploaded_by, play_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)", (sid, unique_name, meta['title'], meta['artist'], meta['album'], meta['duration'], os.stat(path).st_mtime, g.user_email))
        get_db_conn().commit()
        return jsonify({'message': 'Uploaded', 'id': sid}), 201
    os.remove(path)
    return jsonify({'error': 'Metadata error'}), 422

# Deprecated - Handled by heartbeat now, but keeping route structure for existing frontend compatibility until full refresh
@app.route('/api/community', methods=['GET'])
@auth_required
def get_community_data():
    return heartbeat() # Redirect to heartbeat logic basically, or just serve same data format

@app.route('/api/playlists', methods=['GET', 'POST'])
@auth_required
def handle_playlists():
    db = get_db_conn()
    if request.method == 'POST':
        name = request.get_json().get('name', '').strip()
        if not name:
            count = db.execute("SELECT COUNT(*) FROM playlists WHERE user_email=?", (g.user_email,)).fetchone()[0]
            name = f"{g.user_email.split('@')[0]}'s Playlist #{count + 1}"
        pid = str(uuid.uuid4())
        db.execute("INSERT INTO playlists (id, user_email, name, created_at) VALUES (?, ?, ?, ?)", (pid, g.user_email, name, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        db.commit()
        return jsonify({'message': 'Created', 'id': pid, 'name': name}), 201
    else:
        c = db.execute("SELECT p.id, p.name, p.user_email, p.is_public, COUNT(ps.song_id) as song_count FROM playlists p LEFT JOIN playlist_songs ps ON p.id = ps.playlist_id WHERE p.user_email=? GROUP BY p.id, p.name, p.user_email ORDER BY p.name", (g.user_email,))
        playlists = [dict(row) for row in c.fetchall()]
        
        # Inject Upload Status
        for p in playlists:
            if p['id'] in download_status:
                st = download_status[p['id']]
                p['is_uploading'] = True
                p['progress_text'] = f"{st['status']} {st['current']}/{st['total']}"
            else:
                p['is_uploading'] = False
        
        return jsonify({'playlists': playlists}), 200

@app.route('/api/playlists/<pid>', methods=['GET', 'PUT', 'DELETE'])
@auth_required
def handle_playlist_id(pid):
    db = get_db_conn()
    if request.method == 'DELETE':
        # PERMISSION CHECK: Owner OR Admin
        row = db.execute("SELECT user_email FROM playlists WHERE id=?", (pid,)).fetchone()
        if row and (row['user_email'] == g.user_email or g.is_admin):
            db.execute("DELETE FROM playlists WHERE id=?", (pid,))
            db.commit()
            return jsonify({'message': 'Deleted'}), 200
        else:
            return jsonify({'error': 'Denied'}), 403

    elif request.method == 'PUT':
        data = request.get_json()
        # Owner Only for Updates (Admin only deletes for now, unless you want them to rename too)
        if 'name' in data: db.execute("UPDATE playlists SET name=? WHERE id=? AND user_email=?", (data['name'], pid, g.user_email))
        if 'is_public' in data: db.execute("UPDATE playlists SET is_public=? WHERE id=? AND user_email=?", (1 if data['is_public'] else 0, pid, g.user_email))
        db.commit()
        return jsonify({'message': 'Updated'}), 200
    else:
        # Join users to get creator name
        row = db.execute("""
            SELECT p.*, u.display_name 
            FROM playlists p 
            LEFT JOIN users u ON p.user_email = u.email 
            WHERE p.id=?
        """, (pid,)).fetchone()
        
        if not row or (row['user_email'] != g.user_email and not row['is_public'] and not g.is_admin): return jsonify({'error': 'Denied'}), 403
        songs = db.execute("SELECT m.id, m.title, m.artist, m.album, m.duration, m.uploaded_by, m.play_count FROM playlist_songs ps JOIN music m ON ps.song_id = m.id WHERE ps.playlist_id=? ORDER BY m.artist, m.title", (pid,)).fetchall()
        
        creator_name = row['display_name'] if row['display_name'] else row['user_email'].split('@')[0]
        
        return jsonify({
            'id': pid, 
            'name': row['name'], 
            'creator': creator_name, 
            'is_owner': row['user_email']==g.user_email, 
            'is_public': row['is_public'], 
            'created_at': row['created_at'],
            'songs': [dict(s) for s in songs]
        }), 200

@app.route('/api/playlists/<pid>/download', methods=['GET'])
@auth_required
def dl_playlist(pid):
    db = get_db_conn()
    pl = db.execute("SELECT name FROM playlists WHERE id=?", (pid,)).fetchone()
    songs = db.execute("SELECT m.filepath, m.title FROM playlist_songs ps JOIN music m ON ps.song_id = m.id WHERE ps.playlist_id=?", (pid,)).fetchall()
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, 'w', zipfile.ZIP_DEFLATED) as zf:
        for s in songs:
            path = os.path.join(MUSIC_DIR, s['filepath'])
            if os.path.exists(path): zf.write(path, secure_filename(s['title'] + "." + s['filepath'].split('.')[-1]))
    mem.seek(0)
    return send_file(mem, mimetype='application/zip', as_attachment=True, download_name=f"{secure_filename(pl['name'])}.zip")

@app.route('/api/playlists/<pid>/add', methods=['POST'])
@auth_required
def playlist_add(pid):
    data = request.get_json()
    song_ids = data.get('song_ids')
    if not song_ids:
        if data.get('song_id'): song_ids = [data.get('song_id')]
        else: return jsonify({'error': 'No songs'}), 400
    db = get_db_conn()
    if not db.execute("SELECT id FROM playlists WHERE id=? AND user_email=?", (pid, g.user_email)).fetchone(): return jsonify({'error': 'Denied'}), 403
    for sid in song_ids: db.execute("INSERT OR IGNORE INTO playlist_songs (playlist_id, song_id) VALUES (?, ?)", (pid, sid))
    db.commit()
    return jsonify({'message': 'Added'}), 200

@app.route('/api/playlists/<pid>/remove', methods=['POST'])
@auth_required
def playlist_remove(pid):
    data = request.get_json()
    song_ids = data.get('song_ids')
    if not song_ids:
        if data.get('song_id'): song_ids = [data.get('song_id')]
        else: return jsonify({'error': 'No songs'}), 400
    db = get_db_conn()
    
    # PERMISSION CHECK: Owner OR Admin
    row = db.execute("SELECT user_email FROM playlists WHERE id=?", (pid,)).fetchone()
    if row and (row['user_email'] == g.user_email or g.is_admin):
        for sid in song_ids: db.execute("DELETE FROM playlist_songs WHERE playlist_id=? AND song_id=?", (pid, sid))
        db.commit()
        return jsonify({'message': 'Removed'}), 200
    else:
        return jsonify({'error': 'Denied'}), 403

# --- ADMIN ROUTES ---

@app.route('/api/admin/clean_library/scan', methods=['POST'])
@auth_required
def admin_clean_scan():
    if not g.is_admin: return jsonify({'error': 'Admin only'}), 403
    
    db = get_db_conn()
    cursor = db.cursor()
    
    # 1. Scan for Orphan Files (File exists but not in DB)
    orphans = []
    cursor.execute("SELECT filepath FROM music")
    db_filepaths = set([row['filepath'] for row in cursor.fetchall()])
    
    for root, _, files in os.walk(MUSIC_DIR):
        for filename in files:
            if allowed_file(filename):
                full_path = os.path.join(root, filename)
                relative_path = os.path.relpath(full_path, MUSIC_DIR)
                if relative_path not in db_filepaths:
                    orphans.append({'path': relative_path, 'size': os.path.getsize(full_path)})

    # 2. Scan for Duplicates (DB entries with same Title + Artist)
    duplicates = []
    # Find groups of (Title, Artist) having count > 1, ignoring Unknowns
    cursor.execute("""
        SELECT title, artist, COUNT(*) as count 
        FROM music 
        WHERE artist != 'Unknown Artist'
        GROUP BY title, artist 
        HAVING count > 1
    """)
    dup_groups = cursor.fetchall()
    
    for group in dup_groups:
        # Get all IDs for this group, order by created time (simulated by ROWID or just keep one)
        # We'll keep the oldest one (lowest ROWID/import order) and delete the rest
        cursor.execute("SELECT id, title, artist, filepath FROM music WHERE title=? AND artist=? ORDER BY last_modified ASC", (group['title'], group['artist']))
        rows = cursor.fetchall()
        
        # Keep first (index 0), mark rest for deletion
        for i in range(1, len(rows)):
            row = rows[i]
            duplicates.append({'id': row['id'], 'title': row['title'], 'artist': row['artist'], 'path': row['filepath']})

    return jsonify({'orphans': orphans, 'duplicates': duplicates}), 200

@app.route('/api/admin/clean_library/execute', methods=['POST'])
@auth_required
def admin_clean_execute():
    if not g.is_admin: return jsonify({'error': 'Admin only'}), 403
    
    data = request.get_json()
    orphan_paths = data.get('orphans', [])
    duplicate_ids = data.get('duplicates', [])
    
    deleted_files = 0
    deleted_db_rows = 0
    
    db = get_db_conn()
    
    # 1. Delete Orphans (Files only)
    for rel_path in orphan_paths:
        full_path = os.path.join(MUSIC_DIR, rel_path)
        if os.path.exists(full_path):
            try:
                os.remove(full_path)
                deleted_files += 1
            except Exception as e:
                print(f"[ADMIN CLEAN] Failed to delete orphan {rel_path}: {e}")

    # 2. Delete Duplicates (DB + Files)
    for song_id in duplicate_ids:
        # Get path before deleting row
        row = db.execute("SELECT filepath FROM music WHERE id=?", (song_id,)).fetchone()
        if row:
            full_path = os.path.join(MUSIC_DIR, row['filepath'])
            
            # Delete DB Row
            db.execute("DELETE FROM music WHERE id=?", (song_id,))
            db.execute("DELETE FROM playlist_songs WHERE song_id=?", (song_id,))
            deleted_db_rows += 1
            
            # Delete File
            if os.path.exists(full_path):
                try:
                    os.remove(full_path)
                    deleted_files += 1
                except Exception as e:
                    print(f"[ADMIN CLEAN] Failed to delete dup file {row['filepath']}: {e}")

    db.commit()
    return jsonify({'message': f'Cleanup Complete. Deleted {deleted_files} files and {deleted_db_rows} database entries.'}), 200

@app.route('/')
def serve_client(): return send_from_directory(os.getcwd(), CLIENT_HTML_FILENAME)
@app.route(f'/{DOWNLOADER_HTML_FILENAME}')
def serve_dl(): return send_from_directory(os.getcwd(), DOWNLOADER_HTML_FILENAME)
@app.route('/favicon.ico')
def favicon(): return Response(status=204)

init_db()
threading.Thread(target=sync_music_library).start()
observer = start_file_monitoring(app)

if __name__ == '__main__':
    try: app.run(debug=True, host='0.0.0.0', port=PORT, use_reloader=False) 
    finally: observer.stop(); observer.join()