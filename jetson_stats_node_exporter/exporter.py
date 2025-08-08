import logging
import pprint

from prometheus_client.core import GaugeMetricFamily
from .logger import factory
from .jtop_stats import JtopObservable

class Jetson(object):
    def __init__(self, update_period=1):

        if float(update_period) < 0.5:
            raise BlockingIOError("Jetson Stats only works with 0.5s monitoring intervals and slower.")

        self.jtop_observer = JtopObservable(update_period=update_period)
        self.jtop_stats = {}
        self.disk = {}
        self.disk_units = "GB"
        self.interval = update_period

    def update(self):
        self.jtop_stats = self.jtop_observer.read_stats()
        self.disk, self.disk_units = self.jtop_observer.get_storage_info()
        self.network = self.jtop_observer.get_network_bandwidth()


class JetsonExporter(object):

    def __init__(self, update_period):
        self.jetson = Jetson(update_period)
        self.logger = factory(__name__)
        self.name = "Jetson"

    def __cpu(self):
        logging.debug("Starting __cpu() method")
        cpu_data = self.jetson.jtop_stats.get("cpu", {})
        logging.debug(f"Raw jtop_stats['cpu']: {pprint.pformat(cpu_data)}")

        cpu_gauge = GaugeMetricFamily(
            name="cpu_Hz",
            documentation="CPU frequency statistics per core",
            labels=["core", "statistic"],
            unit="Hz"
        )

        cpu_usage = GaugeMetricFamily(
            name="cpu_percent",
            documentation="CPU usage percentage per core and total",
            labels=["core", "mode"],
            unit="percent"
        )

        core_list = cpu_data.get("cpu", [])
        logging.debug(f"Found {len(core_list)} cores")

        for core_number, core_data in enumerate(core_list):
            logging.debug(f"Core {core_number} data: {core_data}")
            if not core_data.get("online", False):
                continue

            freq = core_data.get("freq", {})
            cpu_gauge.add_metric([str(core_number), "cur"], freq.get("cur", 0))
            cpu_gauge.add_metric([str(core_number), "min"], freq.get("min", 0))
            cpu_gauge.add_metric([str(core_number), "max"], freq.get("max", 0))

            cpu_usage.add_metric([str(core_number), "user"], core_data.get("user", 0))
            cpu_usage.add_metric([str(core_number), "nice"], core_data.get("nice", 0))
            cpu_usage.add_metric([str(core_number), "system"], core_data.get("system", 0))
            cpu_usage.add_metric([str(core_number), "idle"], core_data.get("idle", 0))

        total_data = cpu_data.get("total", {})
        logging.debug(f"Total CPU data: {total_data}")
        if total_data:
            cpu_usage.add_metric(["total", "user"], total_data.get("user", 0))
            cpu_usage.add_metric(["total", "nice"], total_data.get("nice", 0))
            cpu_usage.add_metric(["total", "system"], total_data.get("system", 0))
            cpu_usage.add_metric(["total", "idle"], total_data.get("idle", 0))

        metrics = [cpu_gauge, cpu_usage]

        logging.debug(f"__cpu() returning raw metrics: {metrics}")
        for i, metric in enumerate(metrics):
            logging.debug(f"Metric {i} type: {type(metric)}")
            logging.debug(f"Metric {i} dir: {dir(metric)}")
            if hasattr(metric, '__dict__'):
                logging.debug(f"Metric {i} __dict__: {pprint.pformat(metric.__dict__)}")

        logging.debug(f"__cpu() returning metrics: {[type(m) for m in metrics]}")
        return metrics

    def __gpu(self):
        logging.debug("Starting __gpu() method")

        gpu_data = self.jetson.jtop_stats.get("gpu", {}).get("gpu", {})
        status = gpu_data.get("status", {})
        freq = gpu_data.get("freq", {})

        # GPU Load
        load = status.get("load", 0.0)
        logging.debug(f"GPU Load: {load}")

        gpu_util_gauge = GaugeMetricFamily(
            name="gpu_utilization_percentage",
            documentation="GPU load utilization percentage from jtop",
            labels=["gpu"]
        )
        gpu_util_gauge.add_metric(["integrated"], load)

        # Frequency stats
        gpu_freq = GaugeMetricFamily(
            name="gpu_frequency_hz",
            documentation="GPU frequency statistics",
            labels=["gpu", "statistic"],
            unit="Hz"
        )
        gpu_freq.add_metric(["integrated", "cur"], freq.get("cur", 0))
        gpu_freq.add_metric(["integrated", "min"], freq.get("min", 0))
        gpu_freq.add_metric(["integrated", "max"], freq.get("max", 0))

        # Optional: GPC[0] if present
        gpc = freq.get("GPC", [])
        if len(gpc) > 0:
            gpu_freq.add_metric(["integrated", "gpc0"], gpc[0])

        return [gpu_util_gauge, gpu_freq]

    def __ram(self):
        logging.debug("Starting __ram() method")

        ram_data = self.jetson.jtop_stats.get("memory", {}).get("RAM", {})
        logging.debug(f"RAM data: {ram_data}")

        ram_gauge = GaugeMetricFamily(
            name="ram",
            documentation="Memory Statistics from Jetson Stats (unit: kB)",
            labels=["statistic"],
            unit="kB"
        )

        ram_gauge.add_metric(["total"], ram_data.get("tot", 0))
        ram_gauge.add_metric(["used"], ram_data.get("used", 0))
        ram_gauge.add_metric(["free"], ram_data.get("free", 0))
        ram_gauge.add_metric(["buffers"], ram_data.get("buffers", 0))
        ram_gauge.add_metric(["cached"], ram_data.get("cached", 0))
        ram_gauge.add_metric(["shared"], ram_data.get("shared", 0))

        return [ram_gauge]

    def __swap(self):
        swap_gauge = GaugeMetricFamily(
            name="swap",
            documentation=f"Swap Statistics from Jetson Stats",
            labels=["statistic"],
            unit="kB"
        )

        swap_gauge.add_metric(["total"], value=self.jetson.jtop_stats["memory"]["SWAP"]["tot"])
        swap_gauge.add_metric(["used"], value=self.jetson.jtop_stats["memory"]["SWAP"]["used"])
        swap_gauge.add_metric(["cached"], value=self.jetson.jtop_stats["memory"]["SWAP"]["cached"])

        return [swap_gauge]

    def __emc(self):
        emc_gauge = GaugeMetricFamily(
            name="emc",
            documentation=f"EMC Statistics from Jetson Stats",
            labels=["statistic"],
            unit="Hz"
        )

        emc_gauge.add_metric(["total"], value=self.jetson.jtop_stats["memory"]["EMC"]["cur"])
        emc_gauge.add_metric(["used"], value=self.jetson.jtop_stats["memory"]["EMC"]["max"])
        emc_gauge.add_metric(["cached"], value=self.jetson.jtop_stats["memory"]["EMC"]["min"])

        return [emc_gauge]

    def __temperature(self):
        temperature_gauge = GaugeMetricFamily(
            name="temperature",
            documentation=f"Temperature Statistics from Jetson Stats (unit: °C)",
            labels=["statistic", "machine_part", "system_critical"],
            unit="C"
        )
        for part, temp in self.jetson.jtop_stats['temperature'].items():
            temperature_gauge.add_metric([part], value=temp["temp"])

        return [temperature_gauge]

    def __integrated_power_machine_parts(self):
        power_gauge = GaugeMetricFamily(
            name="integrated_power",
            documentation="Power Statistics from internal power sensors (unit: mW/mV/mA)",
            labels=["statistic", "machine_part", "system_critical"]
        )

        for part, reading in self.jetson.jtop_stats["power"]["rail"].items():
            power_gauge.add_metric(["voltage", part], value=reading["volt"])
            power_gauge.add_metric(["current", part], value=reading["curr"])
            power_gauge.add_metric(["critical", part], value=reading["warn"])
            power_gauge.add_metric(["power", part], value=reading["power"])
            power_gauge.add_metric(["avg_power", part], value=reading["avg"])

        return [power_gauge]

    def __integrated_power_total(self):
        power_gauge = GaugeMetricFamily(
            name="integrated_power",
            documentation="Power Statistics from internal power sensors (unit: mW)",
            labels=["statistic", "machine_part", "system_critical"],
            unit="mW"
        )

        power_gauge.add_metric(["power"], value=self.jetson.jtop_stats["power"]["tot"]["power"])
        power_gauge.add_metric(["avg_power"], value=self.jetson.jtop_stats["power"]["tot"]["avg"])

        return [power_gauge]

    def __disk(self):
        disk_gauge = GaugeMetricFamily(
            name="disk",
            documentation=f"Local Storage Statistics from Jetson Stats (unit: {self.jetson.disk_units})",
            labels=["mountpoint", "statistic"],
            unit="GB"
        )
        for mountpoint, disk_info in self.jetson.disk.items():
            if mountpoint == "/":
                disk_gauge.add_metric(["total"], value=disk_info["total"])
                disk_gauge.add_metric(["used"], value=disk_info["used"])
                disk_gauge.add_metric(["free"], value=disk_info["free"])
                disk_gauge.add_metric(["percent"], value=disk_info["percent"])

        return [disk_gauge]

    def __uptime(self):
        uptime_gauge = GaugeMetricFamily(
            name="uptime",
            documentation="Machine Uptime Statistics from Jetson Stats",
            labels=["statistic", "runtime"],
            unit="s"
        )
        uptime_gauge.add_metric(["alive"], value=self.jetson.jtop_stats["uptime"].total_seconds())
        return [uptime_gauge]

    def __network_bandwidth(self):
        network_gauge = GaugeMetricFamily(
            name="network_bandwidth_bytes_per_second",
            documentation="Network bandwidth usage per interface (bytes/sec)",
            labels=["interface", "direction"]
        )

        for iface, stats in self.jetson.network.items():
            network_gauge.add_metric([iface, "rx"], stats["rx_bytes_per_sec"])
            network_gauge.add_metric([iface, "tx"], stats["tx_bytes_per_sec"])

        return [network_gauge]

    def collect(self):
        self.jetson.update()
        yield from self.__cpu()
        yield from self.__gpu()
        yield from self.__ram()
        yield from self.__swap()
        yield from self.__emc()
        yield from self.__temperature()
        yield from self.__integrated_power_machine_parts()
        yield from self.__integrated_power_total()
        yield from self.__disk()
        yield from self.__uptime()
        yield from self.__network_bandwidth()
