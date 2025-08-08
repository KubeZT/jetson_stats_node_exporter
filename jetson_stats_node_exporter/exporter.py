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

        cpu_utilization = GaugeMetricFamily(
            name="cpu_utilization_percent",
            documentation="CPU usage percent per core and total",
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

            cpu_utilization.add_metric([str(core_number), "user"], core_data.get("user", 0))
            cpu_utilization.add_metric([str(core_number), "nice"], core_data.get("nice", 0))
            cpu_utilization.add_metric([str(core_number), "system"], core_data.get("system", 0))
            cpu_utilization.add_metric([str(core_number), "idle"], core_data.get("idle", 0))

        total_data = cpu_data.get("total", {})
        logging.debug(f"Total CPU data: {total_data}")
        if total_data:
            cpu_utilization.add_metric(["total", "user"], total_data.get("user", 0))
            cpu_utilization.add_metric(["total", "nice"], total_data.get("nice", 0))
            cpu_utilization.add_metric(["total", "system"], total_data.get("system", 0))
            cpu_utilization.add_metric(["total", "idle"], total_data.get("idle", 0))

        metrics = [cpu_gauge, cpu_utilization]

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
            name="gpu_utilization_percent",
            documentation="GPU load utilization percent from jtop",
            labels=["gpu"],
            unit="percent"
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

        swap = self.jetson.jtop_stats.get("memory", {}).get("SWAP", {})

        swap_gauge.add_metric(["total"], value=swap.get("tot", 0))
        swap_gauge.add_metric(["used"], value=swap.get("used", 0))
        swap_gauge.add_metric(["cached"], value=swap.get("cached", 0))

        return [swap_gauge]

    def __emc(self):
        emc_gauge = GaugeMetricFamily(
            name="emc",
            documentation=f"EMC Statistics from Jetson Stats",
            labels=["statistic"],
            unit="Hz"
        )

        emc = self.jetson.jtop_stats.get("memory", {}).get("EMC", {})

        emc_gauge.add_metric(["cur"], value=emc.get("cur", 0))
        emc_gauge.add_metric(["max"], value=emc.get("max", 0))
        emc_gauge.add_metric(["min"], value=emc.get("min", 0))

        return [emc_gauge]

    def __temperature(self):
        logging.debug("Starting __temperature() method")

        temperature_data = self.jetson.jtop_stats.get("temperature", {})
        logging.debug(f"Temperature data: {temperature_data}")

        temperature_gauge = GaugeMetricFamily(
            name="temperature",
            documentation="Temperature Statistics from Jetson Stats (unit: °C)",
            labels=["statistic", "machine_part", "system_critical"],
            unit="C"
        )

        for part, temp_info in temperature_data.items():
            temp = temp_info.get("temp", -999)
            online = temp_info.get("online", False)
            system_critical = str(not online or temp <= -255).lower()
            temperature_gauge.add_metric(["temp", part, system_critical], temp)

        return [temperature_gauge]

    def __integrated_power_machine_parts(self):
        logging.debug("Starting __integrated_power_machine_parts() method")

        power_data = self.jetson.jtop_stats.get("power", {}).get("rail", {})
        logging.debug(f"Power rail data: {power_data}")

        voltage_gauge = GaugeMetricFamily(
            name="power_voltage_millivolts",
            documentation="Voltage per power rail",
            labels=["machine_part", "system_critical"],
            unit="mV"
        )

        current_gauge = GaugeMetricFamily(
            name="power_current_milliamps",
            documentation="Current per power rail",
            labels=["machine_part", "system_critical"],
            unit="mA"
        )

        power_gauge = GaugeMetricFamily(
            name="power_consumption_milliwatts",
            documentation="Instantaneous power per power rail",
            labels=["machine_part", "system_critical"],
            unit="mW"
        )

        avg_power_gauge = GaugeMetricFamily(
            name="power_average_milliwatts",
            documentation="Average power per power rail",
            labels=["machine_part", "system_critical"],
            unit="mW"
        )

        warn_threshold_gauge = GaugeMetricFamily(
            name="power_warn_threshold_milliwatts",
            documentation="Warning threshold per power rail",
            labels=["machine_part", "system_critical"],
            unit="mW"
        )

        for part, reading in power_data.items():
            online = reading.get("online", False)
            system_critical = str(not online or reading.get("warn", 0) > 80000).lower()

            voltage_gauge.add_metric([part, system_critical], reading.get("volt", 0))
            current_gauge.add_metric([part, system_critical], reading.get("curr", 0))
            power_gauge.add_metric([part, system_critical], reading.get("power", 0))
            avg_power_gauge.add_metric([part, system_critical], reading.get("avg", 0))
            warn_threshold_gauge.add_metric([part, system_critical], reading.get("warn", 0))

        return [
            voltage_gauge,
            current_gauge,
            power_gauge,
            avg_power_gauge,
            warn_threshold_gauge
        ]

    def __integrated_power_total(self):
        logging.debug("Starting __integrated_power_total() method")

        total_power = self.jetson.jtop_stats.get("power", {}).get("tot", {})
        logging.debug(f"Total power data: {total_power}")

        power_gauge = GaugeMetricFamily(
            name="power_consumption_milliwatts",
            documentation="Total system power consumption (instantaneous)",
            labels=["statistic"],
            unit="mW"
        )

        avg_power_gauge = GaugeMetricFamily(
            name="power_average_milliwatts",
            documentation="Total system average power consumption",
            labels=["statistic"],
            unit="mW"
        )

        power_gauge.add_metric(["power"], total_power.get("power", 0))
        avg_power_gauge.add_metric(["avg_power"], total_power.get("avg", 0))

        return [power_gauge, avg_power_gauge]

    def __disk(self):
        logging.debug("Starting __disk() method")

        disk_data = self.jetson.disk
        logging.debug(f"Disk data: {disk_data}")

        disk_gauge = GaugeMetricFamily(
            name="disk",
            documentation=f"Local Storage Statistics from Jetson Stats (unit: {self.jetson.disk_units})",
            labels=["statistic"],
            unit="GB"
        )

        disk_gauge.add_metric(["total"], value=disk_data.get("total", 0))
        disk_gauge.add_metric(["used"], value=disk_data.get("used", 0))
        disk_gauge.add_metric(["available"], value=disk_data.get("available", 0))
        disk_gauge.add_metric(["available_no_root"], value=disk_data.get("available_no_root", 0))

        return [disk_gauge]

    def __uptime(self):
        logging.debug("Starting __uptime() method")

        uptime = self.jetson.jtop_stats.get("uptime")
        logging.debug(f"Uptime value: {uptime}")

        uptime_gauge = GaugeMetricFamily(
            name="uptime",
            documentation="Machine Uptime Statistics from Jetson Stats",
            labels=["statistic"],
            unit="s"
        )

        uptime_seconds = uptime.total_seconds() if uptime else 0
        uptime_gauge.add_metric(["alive"], value=uptime_seconds)

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
