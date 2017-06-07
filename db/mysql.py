import os
import pickle
import time

import MySQLdb
from MySQLdb.cursors import DictCursor

CACHE_FILENAME = 'results.p'
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")

QUERY = f"""
    SELECT
      io.r0,
      io.r1,
      io.r2,
      io.r3,
      io.w0,
      io.w1,
      io.w2,
      io.w3,
      ((io.r0 + io.r3) / 2) - ((io.r1 + io.r2) / 2) AS caps_mid_diff_read,
      ((io.w0 + io.w3) / 2) - ((io.w1 + io.w2) / 2) AS caps_mid_diff_write,
      io.gpfs_read,
      io.gpfs_write,
      a.name AS appname,
      pip.short_name AS pi,
      j.shared,
      io.res_id,
      j.local_job_id
    FROM
      `ts_analysis`.`iosections` io,
      `modw_supremm`.`job` j,
      `modw_supremm`.`application` a,
      `modw`.`piperson` pip
    WHERE
      io.res_id = j.resource_id AND io.local_job_id = j.local_job_id AND io.end_timestamp = j.end_time_ts
      AND j.datasource_id = 2
      AND (j.end_time_ts - j.start_time_ts) > %s
      AND j.application_id = a.id
      AND j.principalinvestigator_person_id = pip.person_id
      AND j.shared = 0
    """


def get_results(rebuild_cache=False):
    if rebuild_cache or not os.path.isfile(CACHE_FILENAME):
        conn = MySQLdb.connect(host=MYSQL_HOST, port=3306, user=os.environ['MYSQL_USER'],
                               password=os.environ['MYSQL_PW'],
                               cursorclass=DictCursor)

        t = time.perf_counter()
        cur = conn.cursor()
        cur.execute(QUERY, (3600,))
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

