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
# --- SETUP: BOT, WEB SERVER, AUR LOGGING ---
# =====================================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- Lifespan: Server chalu ho raha hai... ---")
    await db.connect()
    try:
        print("Starting main Pyrogram bot...")
        await bot.start()
        me = await bot.get_me()
        Config.BOT_USERNAME = me.username
        print(f"✅ Main Bot [@{Config.BOT_USERNAME}] start ho gaya.")

        multi_clients[0] = bot
        work_loads[0] = 0
        await initialize_clients()
        
        # Ensure Storage Channel ID is correct format
        storage_id = int(Config.STORAGE_CHANNEL)
        print(f"Verifying storage channel ({storage_id})...")
        await bot.get_chat(storage_id)
        print("✅ Storage channel accessible hai.")

        if Config.FORCE_SUB_CHANNEL:
            try:
                await bot.get_chat(Config.FORCE_SUB_CHANNEL)
                print("✅ Force Sub channel accessible hai.")
            except Exception as e:
                print(f"!!! WARNING: Force Sub error: {e}")
        
        print("--- Lifespan: Startup poora hua. ---")
    except Exception as e:
        print(f"!!! FATAL ERROR: {traceback.format_exc()}")
    yield
    if bot.is_initialized: await bot.stop()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class HideDLFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "GET /dl/" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(HideDLFilter())

bot = Client("SimpleStreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)
multi_clients = {}; work_loads = {}; class_cache = {}

# =====================================================================================
# --- MULTI-CLIENT LOGIC ---
# =====================================================================================

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

# =====================================================================================
# --- HELPER FUNCTIONS ---
# =====================================================================================

def get_readable_file_size(size_in_bytes):
    if not size_in_bytes: return '0B'
    power = 1024; n = 0; power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB'}
    while size_in_bytes >= power and n < len(power_labels) - 1:
        size_in_bytes /= power; n += 1
    return f"{size_in_bytes:.2f} {power_labels[n]}"

# =====================================================================================
# --- PYROGRAM BOT HANDLERS ---
# =====================================================================================

@bot.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    if len(message.command) > 1 and message.command[1].startswith("verify_"):
        unique_id = message.command[1].split("_", 1)[1]
        if Config.FORCE_SUB_CHANNEL:
            try: await client.get_chat_member(Config.FORCE_SUB_CHANNEL, message.from_user.id)
            except UserNotParticipant:
                channel_username = str(Config.FORCE_SUB_CHANNEL).replace('@', '')
                btn = InlineKeyboardMarkup([[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{channel_username}")], 
                                           [InlineKeyboardButton("✅ Joined", url=f"https://t.me/{Config.BOT_USERNAME}?start={message.command[1]}")]])
                return await message.reply_text("**You Must Join Our Channel To Get The Link!**", reply_markup=btn)

        final_link = f"{Config.BASE_URL}/show/{unique_id}"
        btn = InlineKeyboardMarkup([[InlineKeyboardButton("Open Link", url=final_link)]])
        await message.reply_text(f"__✅ Verification Successful!\n\nCopy Link:__ `{final_link}`", reply_markup=btn, quote=True)
    else:
        await message.reply_text(f"👋 **Hello, {message.from_user.first_name}!**\nSend any video/file to get links.")

# Step 1: Handle File and Ask for Name (Updated filters for .mkv support)
@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def handle_incoming_file(client, message: Message):
    media = message.document or message.video or message.audio
    if not media: return

    existing_data = await db.collection.find_one({"file_unique_id": media.file_unique_id})
    if existing_data:
        unique_id = existing_data["_id"]
        storage_msg_id = existing_data["message_id"]
        direct_link = f"{Config.BASE_URL}/dl/{storage_msg_id}/file"
        verify_link = f"https://t.me/{Config.BOT_USERNAME}?start=verify_{unique_id}"
        
        btn = InlineKeyboardMarkup([[InlineKeyboardButton("Get Link", url=verify_link)], [InlineKeyboardButton("Stream", url=direct_link)]])
        return await message.reply_text("✅ This file already exists in our database!", reply_markup=btn, quote=True)

    waiting_for_name[message.from_user.id] = message
    await message.reply_text(
        "📝 **Now send the Name for this file.**\n\n"
        "I will automatically add `moviedekhobd.rf.gd` at the beginning and before the extension.",
        quote=True
    )

# Step 2: Receive Name and Process (Fixed Send Method Error)
@bot.on_message(filters.private & filters.text & ~filters.command("start"))
async def process_name_and_upload(client, message: Message):
    user_id = message.from_user.id
    if user_id not in waiting_for_name:
        return

    original_file_message = waiting_for_name.pop(user_id)
    media = original_file_message.document or original_file_message.video or original_file_message.audio
    
    user_input_name = message.text
    original_name = media.file_name or "video.mp4"
    name_part, extension = os.path.splitext(original_name)
    if not extension: extension = ".mkv"

    final_file_name = f"moviedekhobd.rf.gd {user_input_name} moviedekhobd.rf.gd{extension}"

    try:
        sts = await message.reply_text("🚀 Processing and sending to storage...")
        storage_chat = int(Config.STORAGE_CHANNEL)

        # FIXED: Use send_video if it's a video, otherwise send_document
        if original_file_message.video:
            sent_message = await bot.send_video(
                chat_id=storage_chat,
                video=media.file_id,
                file_name=final_file_name,
                caption=f"**File:** `{final_file_name}`"
            )
        else:
            sent_message = await bot.send_document(
                chat_id=storage_chat,
                document=media.file_id,
                file_name=final_file_name,
                caption=f"**File:** `{final_file_name}`"
            )

        unique_id = secrets.token_urlsafe(8)
        storage_msg_id = sent_message.id
        
        await db.collection.insert_one({
            "_id": unique_id, 
            "message_id": storage_msg_id, 
            "file_unique_id": media.file_unique_id
        })

        safe_dl_name = final_file_name.replace(' ', '_')
        direct_link = f"{Config.BASE_URL}/dl/{storage_msg_id}/{safe_dl_name}"
        verify_link = f"https://t.me/{Config.BOT_USERNAME}?start=verify_{unique_id}"

        await sts.edit(
            f"✅ **Success!**\n\n📄 **Name:** `{final_file_name}`\n⚖️ **Size:** `{get_readable_file_size(media.file_size)}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Get Link", url=verify_link)],
                [InlineKeyboardButton("Stream Online", url=direct_link)]
            ])
        )
    except Exception:
        print(f"!!! Error: {traceback.format_exc()}")
        await message.reply_text("❌ Something went wrong while processing.")

# =====================================================================================
# --- FASTAPI WEB SERVER (STREAMING & API) ---
# =====================================================================================

@app.get("/")
async def health_check(): return {"status": "ok", "message": "Server is running!"}

@app.get("/show/{unique_id}", response_class=HTMLResponse)
async def show_page(request: Request, unique_id: str):
    return templates.TemplateResponse("show.html", {"request": request})

@app.get("/api/file/{unique_id}", response_class=JSONResponse)
async def get_file_details_api(request: Request, unique_id: str):
    message_id = await db.get_link(unique_id)
    if not message_id: raise HTTPException(404, "Invalid Link")
    try:
        message = await bot.get_messages(int(Config.STORAGE_CHANNEL), message_id)
        media = message.document or message.video or message.audio
        file_name = media.file_name or "file"
        safe_name = "".join(c for c in file_name if c.isalnum() or c in (' ', '.', '_', '-')).rstrip()
        mime_type = media.mime_type or "application/octet-stream"
        dl_url = f"{Config.BASE_URL}/dl/{message_id}/{safe_name}"
        return {
            "file_name": file_name,
            "file_size": get_readable_file_size(media.file_size),
            "is_media": mime_type.startswith(("video", "audio")),
            "direct_dl_link": dl_url,
            "mx_player_link": f"intent:{dl_url}#Intent;action=android.intent.action.VIEW;type={mime_type};end",
            "vlc_player_link": f"intent:{dl_url}#Intent;action=android.intent.action.VIEW;type={mime_type};package=org.videolan.vlc;end"
        }
    except: raise HTTPException(404, "File not found")

class ByteStreamer:
    def __init__(self,c:Client):self.client=c
    async def yield_file(self,f,i,o,fc,lc,pc,cs):
        c=self.client; work_loads[i]+=1
        ms=c.media_sessions.get(f.dc_id)
        if not ms:
            if f.dc_id!=await c.storage.dc_id():
                ak=await Auth(c,f.dc_id,await c.storage.test_mode()).create(); ms=Session(c,f.dc_id,ak,await c.storage.test_mode(),is_media=True); await ms.start()
                ea=await c.invoke(raw.functions.auth.ExportAuthorization(dc_id=f.dc_id)); await ms.invoke(raw.functions.auth.ImportAuthorization(id=ea.id,bytes=ea.bytes))
            else: ms=c.session
            c.media_sessions[f.dc_id]=ms
        loc=raw.types.InputDocumentFileLocation(id=f.media_id,access_hash=f.access_hash,file_reference=f.file_reference,thumb_size=f.thumbnail_size)
        try:
            for cp in range(1, pc + 1):
                r=await ms.invoke(raw.functions.upload.GetFile(location=loc,offset=o,limit=cs),retries=0)
                if not r.bytes: break
                if pc==1: yield r.bytes[fc:lc]
                elif cp==1: yield r.bytes[fc:]
                elif cp==pc: yield r.bytes[:lc]
                else: yield r.bytes
                o+=cs
        finally: work_loads[i]-=1

@app.get("/dl/{mid}/{fname}")
async def stream_media(r:Request, mid:int, fname:str):
    idx = min(work_loads, key=work_loads.get); c = multi_clients[idx]
    st = class_cache.get(c) or ByteStreamer(c); class_cache[c]=st
    try:
        msg = await c.get_messages(int(Config.STORAGE_CHANNEL), mid)
        m = msg.document or msg.video or msg.audio
        fid=FileId.decode(m.file_id); fsize=m.file_size; rh=r.headers.get("Range",""); fb,ub=0,fsize-1
        if rh:
            rps=rh.replace("bytes=","").split("-"); fb=int(rps[0])
            if len(rps)>1 and rps[1]: ub=int(rps[1])
        rl=ub-fb+1; cs=1024*1024; off=(fb//cs)*cs; fc=fb-off; lc=(ub%cs)+1; pc=math.ceil(rl/cs)
        body=st.yield_file(fid,idx,off,fc,lc,pc,cs)
        hdrs={"Content-Type": m.mime_type or "application/octet-stream", "Accept-Ranges": "bytes", "Content-Length": str(rl), "Content-Disposition": f'inline; filename="{m.file_name}"'}
        if rh: hdrs["Content-Range"]=f"bytes {fb}-{ub}/{fsize}"
        return StreamingResponse(body, status_code=206 if rh else 200, headers=hdrs)
    except: raise HTTPException(404)

@bot.on_chat_member_updated(filters.chat(int(Config.STORAGE_CHANNEL)))
async def simple_gatekeeper(c, m):
    try:
        if m.new_chat_member and m.new_chat_member.status==enums.ChatMemberStatus.MEMBER:
            if m.new_chat_member.user.id not in [Config.OWNER_ID, c.me.id]:
                await c.ban_chat_member(int(Config.STORAGE_CHANNEL), m.new_chat_member.user.id)
                await c.unban_chat_member(int(Config.STORAGE_CHANNEL), m.new_chat_member.user.id)
    except: pass

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
