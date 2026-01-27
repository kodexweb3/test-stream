import os
import asyncio
import secrets
import traceback
import uvicorn
import re
import logging
from contextlib import asynccontextmanager

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated
from pyrogram.errors import FloodWait, UserNotParticipant
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pyrogram.file_id import FileId
from pyrogram import raw
from pyrogram.session import Session, Auth
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import math

# Project files import
from config import Config
from database import db

# temporary storage for file processing
waiting_for_name = {}

# =====================================================================================
# --- SETUP: BOT & WEB SERVER ---
# =====================================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    try:
        await bot.start()
        me = await bot.get_me()
        Config.BOT_USERNAME = me.username
        multi_clients[0] = bot
        work_loads[0] = 0
        await initialize_clients()
        print(f"✅ Bot [@{Config.BOT_USERNAME}] is online!")
    except Exception as e:
        print(f"!!! FATAL ERROR: {traceback.format_exc()}")
    yield
    if bot.is_initialized: await bot.stop()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

bot = Client("SimpleStreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)
multi_clients = {}; work_loads = {}; class_cache = {}

async def start_client(client_id, bot_token):
    try:
        client = await Client(name=str(client_id), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=bot_token, no_updates=True, in_memory=True).start()
        work_loads[client_id] = 0
        multi_clients[client_id] = client
    except Exception as e: print(f"!!! Client {client_id} Error: {e}")

async def initialize_clients():
    tokens = {c+1: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}
    tasks = [start_client(i, token) for i, token in tokens.items()]
    await asyncio.gather(*tasks)

def get_readable_file_size(size_in_bytes):
    if not size_in_bytes: return '0B'
    for unit in ['B','KB','MB','GB']:
        if size_in_bytes < 1024: return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024

# =====================================================================================
# --- BOT HANDLERS: DUPLICATE & MKV FIX ---
# =====================================================================================

@bot.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if len(message.command) > 1 and message.command[1].startswith("verify_"):
        unique_id = message.command[1].split("_", 1)[1]
        final_link = f"{Config.BASE_URL}/show/{unique_id}"
        btn = InlineKeyboardMarkup([[InlineKeyboardButton("Open Page", url=final_link)]])
        await message.reply_text(f"✅ Verified! Your Link: `{final_link}`", reply_markup=btn)
    else:
        await message.reply_text(f"👋 Hi {message.from_user.first_name}! Send any file/video.")

# Step 1: Receiving File (Fixing MKV & Duplicate Check)
@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def handle_incoming_file(client, message: Message):
    media = message.document or message.video or message.audio
    if not media: return

    # --- DUPLICATE CHECK: ডাটাবেসে থাকলে সরাসরি লিঙ্ক দিবে, নাম চাবে না ---
    existing = await db.collection.find_one({"file_unique_id": media.file_unique_id})
    if existing:
        unique_id = existing["_id"]
        msg_id = existing["message_id"]
        # ডাটাবেসে থাকা ফাইলের নাম দিয়ে লিঙ্ক তৈরি
        storage_msg = await bot.get_messages(int(Config.STORAGE_CHANNEL), msg_id)
        saved_name = (storage_msg.document or storage_msg.video).file_name or "video.mkv"
        
        direct_link = f"{Config.BASE_URL}/dl/{msg_id}/{saved_name.replace(' ', '_')}"
        show_link = f"{Config.BASE_URL}/show/{unique_id}"
        
        reply_text = (
            f"✅ **Duplicate Found! Already in Database.**\n\n"
            f"📄 **Name:** `{saved_name}`\n\n"
            f"🔗 **Stream Link:**\n`{direct_link}`\n\n"
            f"📥 **Download Link:**\n`{direct_link}`"
        )
        btns = InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Watch/Download", url=show_link)]])
        return await message.reply_text(reply_text, reply_markup=btns, quote=True)

    # নতুন ফাইল হলে নাম চাবে
    waiting_for_name[message.from_user.id] = message
    await message.reply_text("📝 **এখন ফাইলের জন্য একটি নাম লিখে পাঠান।**\nবট অটোমেটিক প্রিফিক্স ও সাফিক্স যোগ করে নিবে।")

# Step 2: Processing Name & Uploading
@bot.on_message(filters.private & filters.text & ~filters.command("start"))
async def process_name_and_upload(client, message: Message):
    user_id = message.from_user.id
    if user_id not in waiting_for_name: return

    original_msg = waiting_for_name.pop(user_id)
    media = original_msg.document or original_msg.video or original_msg.audio
    
    user_input = message.text
    _, ext = os.path.splitext(media.file_name or "video.mkv")
    if not ext: ext = ".mkv"
    
    final_name = f"moviedekhobd.rf.gd {user_input} moviedekhobd.rf.gd{ext}"

    try:
        sts = await message.reply_text("🚀 Processing...")
        sc_id = int(Config.STORAGE_CHANNEL)

        # Video/Document detection for send method
        if original_msg.video:
            sent = await bot.send_video(chat_id=sc_id, video=media.file_id, file_name=final_name, caption=f"`{final_name}`")
        else:
            sent = await bot.send_document(chat_id=sc_id, document=media.file_id, file_name=final_name, caption=f"`{final_name}`")

        u_id = secrets.token_urlsafe(8)
        await db.collection.insert_one({"_id": u_id, "message_id": sent.id, "file_unique_id": media.file_unique_id})

        d_link = f"{Config.BASE_URL}/dl/{sent.id}/{final_name.replace(' ', '_')}"
        s_link = f"{Config.BASE_URL}/show/{u_id}"

        await sts.delete()
        await original_msg.reply_text(
            f"✅ **File Uploaded!**\n\n📄 **Name:** `{final_name}`\n\n"
            f"🔗 **Stream Link:**\n`{d_link}`\n\n"
            f"📥 **Download Link:**\n`{d_link}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Open Watch Page", url=s_link)]]),
            quote=True
        )
    except:
        await message.reply_text("❌ Error processing file.")

# =====================================================================================
# --- STREAMING ENGINE: OPTIMIZED FOR LOW LATENCY ---
# =====================================================================================

@app.get("/show/{unique_id}", response_class=HTMLResponse)
async def show_page(request: Request, unique_id: str):
    return templates.TemplateResponse("show.html", {"request": request, "id": unique_id})

class ByteStreamer:
    def __init__(self, c): self.client = c
    async def yield_file(self, f, i, o, fc, lc, pc, cs):
        c = self.client; work_loads[i]+=1
        try:
            # Multi-session logic for better speed
            ms = c.media_sessions.get(f.dc_id) or c.session
            loc = raw.types.InputDocumentFileLocation(id=f.media_id, access_hash=f.access_hash, file_reference=f.file_reference, thumb_size=f.thumbnail_size)
            for _ in range(pc):
                r = await ms.invoke(raw.functions.upload.GetFile(location=loc, offset=o, limit=cs))
                if not r.bytes: break
                yield r.bytes[fc:] if _==0 else r.bytes[:lc] if _==pc-1 else r.bytes
                o += cs
        finally: work_loads[i]-=1

@app.get("/dl/{mid}/{fname}")
async def stream_media(r: Request, mid: int, fname: str):
    idx = min(work_loads, key=work_loads.get); c = multi_clients[idx]
    st = class_cache.get(c) or ByteStreamer(c); class_cache[c] = st
    try:
        msg = await c.get_messages(int(Config.STORAGE_CHANNEL), mid)
        m = msg.document or msg.video
        fid = FileId.decode(m.file_id); fsize = m.file_size
        rh = r.headers.get("Range", ""); fb = int(rh.replace("bytes=","").split("-")[0]) if rh else 0
        # Optimization: 512KB chunks for smoother start in slow net
        cs = 512 * 1024; off = (fb//cs)*cs; fc = fb-off; rl = fsize-fb
        body = st.yield_file(fid, idx, off, fc, 0, math.ceil(rl/cs), cs)
        return StreamingResponse(body, status_code=206 if rh else 200, headers={
            "Content-Type": m.mime_type or "video/mp4", "Accept-Ranges": "bytes",
            "Content-Length": str(rl), "Content-Disposition": f'inline; filename="{m.file_name}"'
        })
    except: raise HTTPException(404)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
