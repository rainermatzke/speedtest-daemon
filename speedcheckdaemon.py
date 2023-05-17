import time
import os
import datetime
import sys
import pytz
import speedtest
from pathlib import Path


def speed_sample(dir_samples):

    # actual time
    tz = pytz.timezone('Europe/Berlin')
    today = datetime.datetime.today()
    dt = datetime.datetime(year=today.year, month=today.month, day=today.day,
                           hour=today.hour, minute=today.minute, second=today.second)
    timestamp = str(tz.localize(dt))

    # output file
    csv_file = f'{dt.year}{dt.month:02d}.csv'
    p = Path(dir_samples + "/" + csv_file)

    # run speed check
    s = speedtest.Speedtest()
    s.get_servers([])
    server = s.get_best_server()['url']
    ping = s.results.ping
    download = s.download()
    upload = s.upload()

    # create header
    if not p.exists():
        with open(p, 'w') as f:
            f.write('timestamp,protocol,download,upload,ping\n')

    # create data entry
    with open(p, 'a') as f:
        entry = f"{timestamp},url=\'{server}\',{download},{upload},{ping}"
        f.write(entry + '\n')


def infinity_loop():

    # start after 20 secs
    start_time = time.time() + 20

    # get destination directory from environment
    dir_samples = os.getenv('dir_samples')
    if not dir_samples:
        sys.exit(f'env "dir_samples" not set')

    # check sample directory exists
    if not Path(dir_samples).exists():
        sys.exit(f'{dir_samples} directory does not exist')

    while True:

        # wait until next start time
        d = start_time - time.time()
        time.sleep(d)

        # working job
        speed_sample(dir_samples)

        # wait for next start one hour
        start_time += 60*60

        # ensure non negative waiting time (if loop is running too long)
        if time.time() > start_time:
            start_time = time.time() + 20


if __name__ == '__main__':

    infinity_loop()
