import psutil
from jtop import jtop
import time

class JtopObservable(object):

    def __init__(self, update_period=0.5):
        self.data = {}

        self.prev_net_io = psutil.net_io_counters(pernic=True)
        self.last_net_time = time.time()

        with jtop(interval=update_period) as jetson:
            self.jetson = jetson

    def read_stats(self):
        self.data = {
            "cpu": self.jetson.cpu,
            "disk": self.jetson.disk,
            "fan": self.jetson.fan,
            "gpu": self.jetson.gpu,
            "jetson_clocks": self.jetson.jetson_clocks,
            "memory": self.jetson.memory,
            "nvpmodel": self.jetson.nvpmodel,
            "power": self.jetson.power,
            "processes": self.jetson.processes,
            "stats": self.jetson.stats,
            "temperature": self.jetson.temperature,
            "uptime": self.jetson.uptime
        }
        return self.data

    def get_storage_info(self):
        partitions = psutil.disk_partitions()
        unit = "GB"
        unit_factor = 1_000_000_000

        self.storage_data = {}
        for partition in partitions:
            disk_use = psutil.disk_usage(partition.mountpoint)._asdict()
            if partition.mountpoint not in self.storage_data.keys():
                self.storage_data[partition.mountpoint] = {}

            for metric, value in disk_use.items():
                self.storage_data[partition.mountpoint][metric] = value / unit_factor  # Conversion from B to GB

        return self.storage_data, unit


    def get_network_bandwidth(self):
        now = time.time()
        current = psutil.net_io_counters(pernic=True)
        interval = now - self.last_net_time

        result = {}
        for iface, stats in current.items():
            if iface not in self.prev_net_io:
                continue
            prev_stats = self.prev_net_io[iface]
            rx_rate = (stats.bytes_recv - prev_stats.bytes_recv) / interval
            tx_rate = (stats.bytes_sent - prev_stats.bytes_sent) / interval
            result[iface] = {
                "rx_bytes_per_sec": rx_rate,
                "tx_bytes_per_sec": tx_rate
            }

        self.prev_net_io = current
        self.last_net_time = now
        return result
