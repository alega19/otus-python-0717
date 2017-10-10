import os
import unittest

import pb
import deviceapps_pb2 as dapps
from struct import unpack
import gzip
MAGIC = 0xFFFFFFFF
DEVICE_APPS_TYPE = 1
TEST_FILE = "test.pb.gz"


class TestPB(unittest.TestCase):
    deviceapps = [
        {"device": {"type": "idfa", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7c"},
         "lat": 67.7835424444, "lon": -22.8044005471, "apps": [1, 2, 3, 4]},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": [1, 2]},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": []},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "apps": [1]},
    ]

    def tearDown(self):
        os.remove(TEST_FILE)

    def test_write(self):
        bytes_written = pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        self.assertTrue(bytes_written > 0)

        with gzip.open(TEST_FILE) as fd:
            data = fd.read()
        self.assertEquals(bytes_written, len(data))

        offset = 0
        for el in self.deviceapps:
            magic, dev_apps_type, length = unpack('<IHH', data[offset:offset+8])
            self.assertEquals(magic, MAGIC)
            self.assertEquals(dev_apps_type, DEVICE_APPS_TYPE)

            msg_data = data[offset+8:offset+8+length]
            da = dapps.DeviceApps()
            da.ParseFromString(msg_data)

            da_orig = dapps.DeviceApps()
            da_orig.device.id = el['device']['id']
            da_orig.device.type = el['device']['type']
            da_orig.apps.extend(el['apps'])
            if 'lat' in el:
                da_orig.lat = el['lat']
            if 'lon' in el:
                da_orig.lon = el['lon']

            self.assertEquals(da, da_orig)

            offset += 8 + length

    @unittest.skip("Optional problem")
    def test_read(self):
        pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        for i, d in pb.deviceapps_xread_pb(TEST_FILE):
            self.assertEqual(d, self.deviceapps[i])

