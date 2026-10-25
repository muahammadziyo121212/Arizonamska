# Bir marta kompyuterda ishga tushiring: python make_session.py
# my.telegram.org → API development tools dan API_ID va API_HASH oling.
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = int(input("API_ID: "))
api_hash = input("API_HASH: ").strip()
with TelegramClient(StringSession(), api_id, api_hash) as c:
    print("\nTG_SESSION (MAXFIY! hech kimga bermang):\n")
    print(c.session.save())
