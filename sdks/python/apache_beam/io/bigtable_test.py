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

"""Unit tests for Bigtable sources and sinks."""

import logging
import unittest

from apache_beam.io.bigtable import KeyRangeTracker


class TestKeyRangeTracer(unittest.TestCase):

  def test_key_to_fraction_no_endpoints(self):
    self.assertEqual(KeyRangeTracker._key_to_fraction('\x07'), 7./256)
    self.assertEqual(KeyRangeTracker._key_to_fraction('\xFF'), 255./256)
    self.assertEqual(
        KeyRangeTracker._key_to_fraction('\x01\x02\x03'),
        (2**16 + 2**9 + 3) / (2.0**24))

  def test_key_to_fraction(self):
    self.assertEqual(
        KeyRangeTracker._key_to_fraction('\x87', start='\x80'),
        7./128)

    self.assertEqual(
        KeyRangeTracker._key_to_fraction('\x07', end='\x10'),
        7./16)

    self.assertEqual(
        KeyRangeTracker._key_to_fraction('\x47', start='\x40', end='\x80'),
        7/64.)
    self.assertEqual(
        KeyRangeTracker._key_to_fraction('\x47\x80', start='\x40', end='\x80'),
        15/128.)

  def test_key_to_fraction_common_prefix(self):
    self.assertGreater(
        KeyRangeTracker._key_to_fraction('a' * 100 + 'b', start='a' * 100),
        0)

  def test_key_to_fraction_common_prefix(self):
    self.assertBetween(
        0,
        KeyRangeTracker._key_to_fraction('a' * 100 + 'b', start='a' * 100),
        1e-20)
    self.assertEqual(
        KeyRangeTracker._key_to_fraction('a' * 100, end='a' * 100 + 'b'),
        1)
    self.assertEqual(
        KeyRangeTracker._key_to_fraction('a' * 100 + 'b',
                                         start = 'a' * 100 + 'a',
                                         end='a' * 100 + 'c'),
        0.5)

  def assertBetween(self, a, x, b):
    self.assertGreater(x, a)
    self.assertGreater(b, x)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
