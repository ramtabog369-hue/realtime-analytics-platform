import os
import sys
import requests

def get_token():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set")
    return token

def send_message(text: str, channel: str = "@anton_data_blog"):
    url = f"https://api.telegram.org/bot{get_token()}/sendMessage"
    payload = {"chat_id": channel, "text": text, "parse_mode": "Markdown"}
    r = requests.post(url, json=payload)
    if r.status_code != 200:
        print(f"Error: {r.text}")
    else:
        print("Message sent!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/send_to_telegram.py 'Your text'")
        sys.exit(1)

    channel = os.environ.get("TELEGRAM_CHANNEL", "@anton_data_blog")
    text = sys.argv[1]
    send_message(text, channel)
