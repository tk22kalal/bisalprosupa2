import os
import sys
import time
from datetime import datetime, timedelta
import pytz

IST = pytz.timezone('Asia/Kolkata')
UPDATE_HOURS = [0, 6, 12, 18]
UPDATE_MINUTE = 0

def update_date_file():
    now_ist = datetime.now(IST)
    timestamp = now_ist.strftime('%Y-%m-%d %H:%M:%S IST')
    with open('date.txt', 'w') as f:
        f.write(f"Last updated: {timestamp}\n")
    print(f"[{timestamp}] date.txt updated successfully")
    return timestamp

def get_next_run_time():
    now = datetime.now(IST)
    next_hour = None
    for hour in UPDATE_HOURS:
        target_time = now.replace(hour=hour, minute=UPDATE_MINUTE, second=0, microsecond=0)
        if now < target_time:
            next_hour = hour
            next_run = target_time
            break
    if next_hour is None:
        next_run = now.replace(hour=UPDATE_HOURS[0], minute=UPDATE_MINUTE, second=0, microsecond=0)
        next_run += timedelta(days=1)
    time_diff = (next_run - now).total_seconds()
    return time_diff

def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        print("Running in one-time update mode...")
        update_date_file()
        return
    print("Date.txt Auto-Updater Started")
    while True:
        try:
            wait_seconds = get_next_run_time()
            time.sleep(wait_seconds)
            update_date_file()
            time.sleep(60)
        except KeyboardInterrupt:
            print("\nScheduler stopped by user")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()

