import os
import re
import datetime
import sys
import pandas as pd
import pytz
from pathlib import Path

"""
Import speed test logfiles
only run once and check result logs in 
Import Directory is ./logs.old
Export Directory is to be set by environment variable dir_samples
"""


# class for reading the old log files
class LogfileReader:

    # open logfile and read all lines at once
    def __init__(self, path: Path):

        self._ix = -1 # actual line number
        self._path = path

        with open(str(path)) as fh:
            self._lines = fh.read().splitlines()

    # internal function to read next line
    def _next(self):
        self._ix += 1
        if self._ix < len(self._lines):
            return self._lines[self._ix]
        else:
            return None

    # internal function to push line back (e. g. after error)
    def _lineBack(self, line):

        # if actual line is a date we can try to start reading here - put it back
        if DateConverter.contains_weekday(line):
            if self._ix > 0:
                self._ix -= 1

    # get number of lines left
    def lines_left(self):
        return len(self._lines) - self._ix

    # get protocol entry for dataset (logfile name and line number)
    def protocol(self):

        # give hint to data origin
        return f"{self._path.name}@{self._ix}"

    # read a ping measurement
    def readPing(self):

        ping = self._next()
        if not ping.startswith('Ping: '):
            # no ping info in dataset (error)
            self._lineBack(ping)
            return None

        return ping[6:-3]

    # read a download measurement
    def readDownload(self):

        download = self._next()
        if not download.startswith('Download: '):
            # no download info in dataset (error)
            self._lineBack(download)
            return None

        return float(download[10:-7]) * 1024 * 1024

    # read an upload measurement
    def readUpload(self):

        upload = self._next()
        if not upload.startswith('Upload: '):
            # no upload info in dataset (error)
            self._lineBack(upload)
            return None

        return float(upload[8:-7]) * 1024 * 1024

    # read a timestamp
    def readTimestamp(self):

        timestamp = self._next()
        if not DateConverter.contains_weekday(timestamp):
            return None

        return DateConverter.timestamp(timestamp)


# class for date conversion (all sorts of dates that are part of old logfiles
class DateConverter:

    # different given month formats mapped to month id
    _month_dict = {
        'Jan': 1,
        'Feb': 2,
        'Mär': 3, 'Mar': 3,
        'Apr': 4,
        'Mai': 5, 'May': 5,
        'Jun': 6,
        'Jul': 7,
        'Aug': 8,
        'Sep': 9,
        'Okt': 10, 'Oct': 10,
        'Nov': 11,
        'Dez': 12, 'Dec': 12,
    }

    # pattern for reading timestamps (two variants)
    _pattern1 = re.compile(r'.*\w+ (\w+) +(\d+) (\d\d):(\d\d):(\d\d) CES??T (\d{4})$')
    _pattern2 = re.compile(r'.*\w+ (\d+)\. +(\w+) (\d\d):(\d\d):(\d\d) CES??T (\d{4})$')
    _tz = pytz.timezone('Europe/Berlin')

    # check if line contains a timestamp
    @staticmethod
    def contains_weekday(line):

        return line.split(' ')[0] in (
            'Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')

    # convert string to timestamp
    @classmethod
    def timestamp(cls, time_string):

        month = -1
        day = -1

        m = cls._pattern1.match(time_string)
        if m:
            month = cls._month_dict[m.group(1)]
            day = m.group(2)
        else:
            m = cls._pattern2.match(time_string)
            if m:
                month = cls._month_dict[m.group(2)]
                day = m.group(1)

        if not m:
            # error - no match
            return None

        dt = datetime.datetime(
            year=int(m.group(6)), month=month, day=int(day),
            hour=int(m.group(3)), minute=int(m.group(4)), second=int(m.group(5)))
        return cls._tz.localize(dt)


# file object
class FileObject:

    def __init__(self, path, filename):

        self._path = Path(f'{path}/{filename}')
        self._name = filename

    def path(self):

        return self._path

    def name(self):

        return self._name

    def isNewer(self, obj):

        return self.path().stat().st_mtime > obj.path().stat().st_mtime


# directory of files
class DirObject:

    def __init__(self, path, extension=None):

        files = os.listdir(path)
        self._files = []
        for filename in files:
            if extension and not filename.endswith(extension):
                continue
            self._files.append(FileObject(path, filename))

    def list(self):

        return self._files

    def findByName(self, filename):

        for f in self._files:
            if f.path().name == filename:
                return f

        return None


# class to read old logfiles an convert them to Panda Dataframe
class DataframeConverter:

    def __init__(self):

        # get destination directory from environment
        self._csv_dir = os.getenv('dir_samples')
        if not self._csv_dir:
            sys.exit(f'env "dir_samples" not set')

        # check sample directory exists
        if not Path(self._csv_dir).exists():
            sys.exit(f'{self._csv_dir} directory does not exist')

        self._csv_obj = DirObject(self._csv_dir, extension='.csv')

        old_log_dir = os.getcwd() + '/logs.old'
        self._log_obj = DirObject(old_log_dir, extension='.log')

    @classmethod
    def convertToCSV(cls, log_path: Path, csv_path: Path):

        errors = 0
        converted = 0
        skipped = 0

        log_reader = LogfileReader(log_path)

        # either load existing csv or start a new one
        if csv_path.is_file():
            # load existing file
            dataframe = pd.read_csv(str(csv_path), index_col=False)
        else:
            # create new file
            dataframe = pd.DataFrame(columns=['timestamp', 'protocol', 'download', 'upload', 'ping'])

        while log_reader.lines_left() >= 3:

            timestamp = log_reader.readTimestamp()
            if timestamp is None:
                # error
                errors += 1
                continue

            ping = log_reader.readPing()
            if ping is None:
                # error
                errors += 1
                continue

            download = log_reader.readDownload()
            if download is None:
                # error
                errors += 1
                continue

            upload = log_reader.readUpload()
            if upload is None:
                # error
                errors += 1
                continue

            protocol = log_reader.protocol()

            if len(dataframe[dataframe['timestamp'] == str(timestamp)]) < 1:

                # TODO insert line number into protocol
                # insert no duplicates - key is timestamp
                dataframe.loc[len(dataframe)] = {
                    'timestamp': timestamp, 'protocol': protocol,
                    'download': download, 'upload': upload, 'ping': ping}

                converted += 1

            else:

                skipped += 1

        # write csv
        dataframe.to_csv(str(csv_path), index=False)

        # summary
        print(f'converted entries ({converted}), skipped lines ({skipped}), error lines ({errors})')

    def processLogs(self):

        # collect all available logfiles
        all_log_files = self._log_obj.list()

        # filter only logfiles newer than csv result file
        log2csv_map = {}
        for log_file in all_log_files:

            # search corresponding csv file (year based naming)
            csv_filename = f'{log_file.path().name[0:6]}.csv'
            csv_file = self._csv_obj.findByName(csv_filename)

            if csv_file is None:
                # csv file does not exist - generate entries
                log2csv_map[log_file] = Path(f'{self._csv_dir}/{csv_filename}')
            elif log_file.isNewer(csv_file):
                # csv is older than log file - append entries
                log2csv_map[log_file] = Path(f'{self._csv_dir}/{csv_filename}')
            else:
                # skip if csv file is newer
                pass

        # process all marked files
        for log_file in log2csv_map.keys():

            # start process of conversion
            print(f"converting {log_file.path().name}: ", end='')
            self.convertToCSV(log_file.path(), log2csv_map[log_file])


def run_once():
    DataframeConverter().processLogs()

# start here
run_once()

