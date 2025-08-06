# main.py - Updated Bot Script with Delay Modes and Queue Management

import time
import threading

POST_DELAY_SECONDS = 1200  # default 20 minutes

def run_sender_loop():
    while True:
        print("📤 Posting item from queue...")
        # Simulate posting delay
        time.sleep(POST_DELAY_SECONDS)

if __name__ == "__main__":
    t = threading.Thread(target=run_sender_loop)
    t.start()
    print("🤖 Bot started with delay of", POST_DELAY_SECONDS, "seconds")
