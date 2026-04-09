"""
LINE Messaging API - Push message to users
Uses LINE Channel Access Token from environment.
"""
import os, requests, json

LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_USER_IDS = os.environ.get("LINE_USER_IDS", "")  # comma-separated
LINE_API_URL = "https://api.line.me/v2/bot/message/push"


def push_line_message(message: str):
    """Push a text message to all configured LINE user IDs."""
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("WARNING: LINE_CHANNEL_ACCESS_TOKEN not set, skipping push")
        return

    user_ids = [uid.strip() for uid in LINE_USER_IDS.split(",") if uid.strip()]
    if not user_ids:
        print("WARNING: LINE_USER_IDS not set, skipping push")
        return

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }

    # LINE has 5000 char limit per message; split if needed
    chunks = _split_message(message, 4900)

    for uid in user_ids:
        for chunk in chunks:
            payload = {
                "to": uid,
                "messages": [{"type": "text", "text": chunk}]
            }
            try:
                r = requests.post(LINE_API_URL, headers=headers,
                                  data=json.dumps(payload), timeout=30)
                if r.status_code == 200:
                    print(f"LINE push OK -> {uid[:8]}...")
                else:
                    print(f"LINE push FAIL ({r.status_code}): {r.text}")
            except Exception as e:
                print(f"LINE push error: {e}")


def _split_message(msg, max_len):
    """Split a long message into chunks at line boundaries."""
    if len(msg) <= max_len:
        return [msg]
    chunks = []
    current = ""
    for line in msg.split("\n"):
        if len(current) + len(line) + 1 > max_len:
            chunks.append(current)
            current = line
        else:
            current = current + "\n" + line if current else line
    if current:
        chunks.append(current)
    return chunks
