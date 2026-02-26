import types
import unittest
from collections import namedtuple
from unittest import mock

import config
from source.collectors import cpu_memory
from source.collectors import latency_monitor
from source.collectors import error_tracker
from source.collectors import disk_io


class TestCpuMemoryCollector(unittest.TestCase):
    def test_collect_cpu_memory(self):
        with mock.patch("source.collectors.cpu_memory.psutil.cpu_percent", return_value=12.5) as cpu_mock:
            vm = types.SimpleNamespace(percent=45.0)
            with mock.patch("source.collectors.cpu_memory.psutil.virtual_memory", return_value=vm) as mem_mock:
                cpu, memory = cpu_memory.collect_cpu_memory()

        self.assertEqual(cpu, 12.5)
        self.assertEqual(memory, 45.0)
        cpu_mock.assert_called_once_with(interval=None)
        mem_mock.assert_called_once()


class TestLatencyMonitor(unittest.TestCase):
    def test_collect_latency_success(self):
        response = types.SimpleNamespace(status_code=200)
        with mock.patch("source.collectors.latency_monitor.requests.get", return_value=response) as get_mock:
            with mock.patch("source.collectors.latency_monitor.time.time", side_effect=[100.0, 100.123]):
                latency, status = latency_monitor.collect_latency()

        self.assertAlmostEqual(latency, 123.0, places=3)
        self.assertEqual(status, 200)
        get_mock.assert_called_once_with(config.API_URL, timeout=2)

    def test_collect_latency_exception(self):
        with mock.patch("source.collectors.latency_monitor.requests.get", side_effect=RuntimeError("boom")):
            latency, status = latency_monitor.collect_latency()

        self.assertIsNone(latency)
        self.assertEqual(status, 500)


class TestErrorTracker(unittest.TestCase):
    def setUp(self):
        error_tracker._request_history.clear()

    def test_error_rate_empty(self):
        self.assertEqual(error_tracker.get_error_rate(), 0.0)

    def test_error_rate_updates(self):
        error_tracker.update_error_rate(200)
        error_tracker.update_error_rate(500)
        error_tracker.update_error_rate(200)

        self.assertAlmostEqual(error_tracker.get_error_rate(), 1 / 3)

    def test_window_size_respected(self):
        for code in [500, 500, 200, 200]:
            error_tracker.update_error_rate(code)

        # WINDOW_SIZE=3 so only the last 3 values are kept: 500, 200, 200
        self.assertAlmostEqual(error_tracker.get_error_rate(), 1 / 3)


class TestDiskIOCollector(unittest.TestCase):
    def test_collect_disk_io(self):
        Counters = namedtuple("Counters", ["read_bytes", "write_bytes"])
        baseline = Counters(read_bytes=0, write_bytes=0)
        current = Counters(read_bytes=1 * 1024 * 1024, write_bytes=2 * 1024 * 1024)

        disk_io._prev = baseline
        disk_io._prev_time = 10.0

        with mock.patch("source.collectors.disk_io.psutil.disk_io_counters", return_value=current):
            with mock.patch("source.collectors.disk_io.time.time", return_value=12.0):
                read_mb_s, write_mb_s = disk_io.collect_disk_io()

        self.assertAlmostEqual(read_mb_s, 0.5)
        self.assertAlmostEqual(write_mb_s, 1.0)
        self.assertEqual(disk_io._prev, current)
        self.assertEqual(disk_io._prev_time, 12.0)


if __name__ == "__main__":
    unittest.main()
