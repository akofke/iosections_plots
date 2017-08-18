import os
import pickle
import time

import MySQLdb
from MySQLdb.cursors import DictCursor

CACHE_FILENAME = 'results.p'
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")

QUERY = f"""
    SELECT
      io.*,
      a.name AS appname,
      pip.short_name AS pi,
      j.shared,
      j.local_job_id,
      j.wall_time,
      exe.`binary` AS bin
    FROM
      `ts_analysis`.`ts_patterns` io,
      `modw_supremm`.`job` j,
      `modw_supremm`.`application` a,
      `modw`.`piperson` pip,
      `modw_supremm`.`executable` exe 
    WHERE
      io.res_id = j.resource_id AND io.local_job_id = j.local_job_id AND io.end_timestamp = j.end_time_ts
      AND j.datasource_id = 2
      AND (j.end_time_ts - j.start_time_ts) > %s
      AND j.application_id = a.id
      AND j.principalinvestigator_person_id = pip.person_id
      AND j.executable_id = exe.id
      AND j.shared = 0
    """


def get_results(rebuild_cache=False):
    if rebuild_cache or not os.path.isfile(CACHE_FILENAME):
        conn = MySQLdb.connect(host=MYSQL_HOST, port=3306, user=os.environ['MYSQL_USER'],
                               password=os.environ['MYSQL_PW'],
                               cursorclass=DictCursor)

        t = time.perf_counter()
        cur = conn.cursor()
        cur.execute(QUERY, (10 * 60,))
        print(f"Executed mysql query in {time.perf_counter() - t} sec")
        results = cur.fetchall()
        cur.close()

        with open(CACHE_FILENAME, 'wb') as cache:
            pickle.dump(results, cache)

    else:
        with open(CACHE_FILENAME, 'rb') as cache:
            results = pickle.load(cache)

    return results


class Results:

    def __init__(self, rebuild_cache=False):
        self._data = get_results(rebuild_cache=rebuild_cache)

    def filter(self, func):
        self._data = list(filter(func, self.data))

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value

    def rebuild_cache(self):
        self._data = get_results(True)

