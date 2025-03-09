from datetime import datetime
import pytz

myt_tz = pytz.timezone('Asia/Singapore')
now_date = datetime.now(myt_tz).strftime('%Y-%m-%d')
