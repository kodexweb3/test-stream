import os, asyncio, secrets, traceback, uvicorn, re, math
from contextlib import asynccontextmanager
from pyrogram import Client, filters, raw
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.file_id import FileId
from pyrogram.session import Session, Auth
from pyrogram.errors import FileMigrate
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from config import Config
from database import db

waiting_for_name = {}
multi_clients = {}; work_loads = {}; class_cache = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    try:
        await bot.start()
        me = await bot.get_me()
        Config.BOT_USERNAME = me.username
        multi_clients[0] = bot
        work_loads[0] = 0
        tokens = {c+1: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}
        for i, token in tokens.items():
            try:
                c = await Client(name=str(i), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=token, no_updates=True, in_memory=True).start()
                multi_clients[i] = c; work_loads[i] = 0
            except: pass
        print(f"✨ Bot @{Config.BOT_USERNAME} is Live!")
    except Exception: print(traceback.format_exc())
    yield
    if bot.is_initialized: await bot.stop()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["Range", "Content-Type", "Referer", "Origin"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length"],
    max_age=600,
)

bot = Client("SimpleStreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)

def get_readable_size(size):
    for unit in ['B','KB','MB','GB']:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024

# --- BOT HANDLERS ---
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    await message.reply_text(f"👋 **Hello {message.from_user.first_name}!**\n\n📂 Send me any video for high-speed streaming links.")

@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def handle_file(client, message):
    media = message.document or message.video or message.audio
    ex = await db.collection.find_one({"file_unique_id": media.file_unique_id})
    if ex:
        u_id = ex["_id"]; m_id = ex["message_id"]
        f_name = ex.get("file_name", "video.mkv").replace(" ", "_")
        d_link = f"{Config.BASE_URL}/dl/{m_id}/{f_name}"
        return await message.reply_text(
            f"✅ **File already exists!**\n\n🔗 **Stream Link (Click to Copy):**\n`{d_link}`\n\n📥 **Download Link (Click to Copy):**\n`{d_link}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Watch Online", url=f"{Config.BASE_URL}/show/{u_id}")]])
        )
    waiting_for_name[message.from_user.id] = message
    await message.reply_text("📝 **Please send a Name for the file:**")

@bot.on_message(filters.private & filters.text & ~filters.command("start"))
async def process_name(client, message):
    uid = message.from_user.id
    if uid not in waiting_for_name: return
    orig = waiting_for_name.pop(uid)
    media = orig.document or orig.video or orig.audio
    ext = os.path.splitext(media.file_name or ".mkv")[1] or ".mkv"
    user_input = message.text.replace(" ", "_")
    final_name = f"moviedekhobd.rf.gd_{user_input}_moviedekhobd.rf.gd{ext}"
    
    sts = await message.reply_text("🚀 **Uploading to Storage...**")
    sc = int(Config.STORAGE_CHANNEL)
    try:
        sent = await bot.send_video(sc, media.file_id, file_name=final_name, caption=final_name) if orig.video else await bot.send_document(sc, media.file_id, file_name=final_name, caption=final_name)
        u_id = secrets.token_urlsafe(8)
        await db.collection.insert_one({"_id": u_id, "message_id": sent.id, "file_unique_id": media.file_unique_id, "file_name": final_name})
        d_link = f"{Config.BASE_URL}/dl/{sent.id}/{final_name}"
        await sts.delete()
        await orig.reply_text(
            f"✅ **Success!**\n\n🔗 **Stream Link (Click to Copy):**\n`{d_link}`\n\n📥 **Download Link (Click to Copy):**\n`{d_link}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Watch Online", url=f"{Config.BASE_URL}/show/{u_id}")]])
        )
    except: await message.reply_text("❌ **Failed!**")

# --- CLEAN & FAST STREAMING ENGINE ---
class ByteStreamer:
    def __init__(self, c): self.client = c
    async def get_session(self, dc_id):
        if dc_id not in self.client.media_sessions:
            if dc_id == await self.client.storage.dc_id(): session = self.client.session
            else:
                auth = await Auth(self.client, dc_id, await self.client.storage.test_mode()).create()
                session = Session(self.client, dc_id, auth, await self.client.storage.test_mode(), is_media=True)
                await session.start()
                try:
                    exp = await self.client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                    await session.invoke(raw.functions.auth.ImportAuthorization(id=exp.id, bytes=exp.bytes))
                except: pass
            self.client.media_sessions[dc_id] = session
        return self.client.media_sessions[dc_id]

    async def yield_file(self, f, i, o, fc, lc, pc, cs):
        work_loads[i] += 1
        try:
            session = await self.get_session(f.dc_id)
            loc = raw.types.InputDocumentFileLocation(id=f.media_id, access_hash=f.access_hash, file_reference=f.file_reference, thumb_size=f.thumbnail_size)
            for _ in range(pc):
                try:
                    r = await session.invoke(raw.functions.upload.GetFile(location=loc, offset=o, limit=cs))
                    if not r or not r.bytes: break
                    yield r.bytes[fc:] if _==0 else r.bytes[:lc] if _==pc-1 else r.bytes
                except Exception: break
                o += cs
                if _ % 4 == 0: await asyncio.sleep(0.01) # Small delay to prevent HTTP/2 congestion
        finally: work_loads[i] -= 1

@app.get("/dl/{mid}/{fname}")
async def stream_media(r: Request, mid: int, fname: str):
    idx = min(work_loads, key=work_loads.get); c = multi_clients[idx]
    st = class_cache.get(c) or ByteStreamer(c); class_cache[c] = st
    try:
        msg = await c.get_messages(int(Config.STORAGE_CHANNEL), mid)
        m = msg.document or msg.video
        fid = FileId.decode(m.file_id)
        rh = r.headers.get("Range", ""); fb = int(rh.replace("bytes=","").split("-")[0]) if rh else 0
        cs = 1024 * 512 # 512 KB chunks
        off = (fb//cs)*cs; fc = fb-off; rl = m.file_size-fb
        
        return StreamingResponse(
            st.yield_file(fid, idx, off, fc, 0, math.ceil(rl/cs), cs),
            status_code=206 if rh else 200,
            headers={
                "Content-Type": m.mime_type or "video/mp4",
                "Accept-Ranges": "bytes",
                "Content-Length": str(rl),
                "Content-Range": f"bytes {fb}-{m.file_size-1}/{m.file_size}",
                "Connection": "keep-alive",
                "Cache-Control": "no-cache"
            }
        )
    except: raise HTTPException(404)

@app.get("/show/{unique_id}", response_class=HTMLResponse)
async def show_page(request: Request, unique_id: str):
    return templates.TemplateResponse("show.html", {"request": request, "id": unique_id})

@app.get("/api/file/{unique_id}")
async def get_api_data(unique_id: str):
    data = await db.collection.find_one({"_id": unique_id})
    if not data: return JSONResponse({"error": "Not Found"}, status_code=404)
    msg = await bot.get_messages(int(Config.STORAGE_CHANNEL), data["message_id"])
    media = msg.document or msg.video
    f_name = data.get("file_name", media.file_name or "video.mkv")
    return {"file_name": f_name, "file_size": get_readable_size(media.file_size), "is_media": True, "direct_dl_link": f"{Config.BASE_URL}/dl/{data['message_id']}/{f_name}"}

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=10000)
