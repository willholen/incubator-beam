#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A source and a sink for reading from and writing to Google Cloud Bigtable."""

from __future__ import absolute_import

import threading

from apache_beam import coders
from apache_beam.io import iobase
from apache_beam.transforms import PTransform
try:
  from google.cloud import bigtable
except:
  pass

__all__ = ['ReadFromBigtable']


class KeyRangeTracker(iobase.RangeTracker):

  def __init__(self, start_key=None, stop_key=None):
    self._start_key = start_key
    self._stop_key = stop_key
    self._lock = threading.Lock()
    self._last_claim = None

  def start_position(self):
    return self._start_key

  def stop_position(self):
    return self._stop_key

  def try_claim(self, position):  # pylint: disable=unused-argument
    with self._lock:
      return self.stop_key is None or position < self.stop_key
      self._last_claim = position

  def position_at_fraction(self, fraction):
    return self._fraction_to_key(fraction, self._start_key, self._end_key)

  def try_split(self, position):
    with self._lock:
      if self._last_claim < position:
        prev_stop, self._stop_key = self._stop_key, position
        return position, self._key_to_fraction(
            position, self._start_key, prev_stop)
      else:
        return None

  def fraction_consumed(self):
    if self._last_claim is None:
      return 0
    else:
      return self._key_to_fraction(
          self._last_claim, self._start_key, self._end_key)

  @staticmethod
  def _strip_common_prefix(*args):
    for ix, cs in enumerate(zip(*args)):
      if len(set(cs)) != 1:
        break
    return ix, [arg[ix:] for arg in args]

  @staticmethod
  def _fraction_to_key(fraction, start=None, end=None):
    if fraction == 1:
      return end
    elif start is None and end is None:
      m, e = fraction.hex()[2:].split('p')
      return ('0' * (1-int(e)) + m.replace('.', '')).decode('hex')

  @staticmethod
  def _key_to_fraction(key, start=None, end=None):
    if start is None and end is None:
      return float.fromhex('.' + key.encode('hex'))
    elif end is None:
      ix, (key, start) = KeyRangeTracker._strip_common_prefix(key, start)
      start_frac = KeyRangeTracker._key_to_fraction(start)
      key_frac = KeyRangeTracker._key_to_fraction(key)
      return 256.0 ** -ix * (key_frac - start_frac) / (1.0 - start_frac)
    elif start is None:
      key_frac = KeyRangeTracker._key_to_fraction(key)
      end_frac = KeyRangeTracker._key_to_fraction(end)
      return key_frac / end_frac
    else:
      _, (key, start, end) = KeyRangeTracker._strip_common_prefix(key, start, end)
      start_frac = KeyRangeTracker._key_to_fraction(start)
      key_frac = KeyRangeTracker._key_to_fraction(key)
      end_frac = KeyRangeTracker._key_to_fraction(end)
      return (key_frac - start_frac) / (end_frac - start_frac)


class _BigtableSource(iobase.BoundedSource):

  def __init__(self, table_id, instance_id, project_id):
    global bigtable
    from google.cloud import bigtable
    self._table_id = table_id
    self._project_id = project_id
    self._instance_id = instance_id

  def get_table(self):
    client = bigtable.Client(project=self._project_id)
    instance = client.instance(self._instance_id)
    return instance.table(self._table_id)

  def estimate_size(self):
    return max(key.offset_bytes for key in get_table().sample_row_keys())

  def split(self, unused_desired_bundle_size, start_position=None, stop_position=None):
    count = 0
    start_size = 0
    start_key = start_position
    for key in get_table().sample_row_keys():
      count += 1
      stop_key = key.row_key
      if start_key and start_key > stop_key:
        continue
      if stop_position and stop_key > stop_position:
        stop_key = stop_position
      yield iobase.SourceBundle(
          key.offset_bytes - start_size,
          _BigtableSource(self._table_id, self._instance_id, self._project_id),
           start_key, stop_key)
      start_key = stop_key
      start_size = key.offset_bytes
    yield iobase.SourceBundle(
        start_size // count,
        _BigtableSource(self._table_id, self._instance_id, self._project_id),
         start_key, None)

  def get_range_tracker(self, start_position, stop_position):
    return KeyRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    for row in read_rows(range_tracker.start_position(), range_tracker.stop_position()):
      key = row.row_key()
      if range_tracker.try_claim(key):
        yield key, row.dict()
      else:
        break

class ReadBigtable(PTransform):
  def __init__(self, table_id, instance_id, project_id):
    self._args = (table_id, instance_id, project_id)

  def apply(self, p):
    return p | iobase.Read(_BigtableSource(*self._args))
