import logging
import pprint
import socket

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
        self.name = socket.gethostname().split(".")[0]

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
            labels=["machine_part", "online", "critical"],
            unit="mV"
        )

        current_gauge = GaugeMetricFamily(
            name="power_current_milliamps",
            documentation="Current per power rail",
            labels=["machine_part", "online", "critical"],
            unit="mA"
        )

        power_gauge = GaugeMetricFamily(
            name="power_consumption_milliwatts",
            documentation="Instantaneous power per power rail",
            labels=["machine_part", "online", "critical"],
            unit="mW"
        )

        avg_power_gauge = GaugeMetricFamily(
            name="power_average_milliwatts",
            documentation="Average power per power rail",
            labels=["machine_part", "online", "critical"],
            unit="mW"
        )

        warn_threshold_gauge = GaugeMetricFamily(
            name="power_warn_threshold_milliwatts",
            documentation="Warning threshold per power rail",
            labels=["machine_part", "online", "critical"],
            unit="mW"
        )

        for part, reading in power_data.items():
            online_val = reading.get("online", False)
            crit_val = reading.get("crit", 0)

            online_str = str(online_val).lower()
            crit_str = str(bool(crit_val)).lower()

            voltage_gauge.add_metric([part, online_str, crit_str], reading.get("volt", 0))
            current_gauge.add_metric([part, online_str, crit_str], reading.get("curr", 0))
            power_gauge.add_metric([part, online_str, crit_str], reading.get("power", 0))
            avg_power_gauge.add_metric([part, online_str, crit_str], reading.get("avg", 0))
            warn_threshold_gauge.add_metric([part, online_str, crit_str], reading.get("warn", 0))

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

    def __fan(self):
        logging.debug("Starting __fan() method")

        fan_data = self.jetson.jtop_stats.get("fan", {}).get("pwmfan", {})
        logging.debug(f"Fan data: {fan_data}")

        fan_speed_gauge = GaugeMetricFamily(
            name="fan_speed_percent",
            documentation="Fan PWM speed (percentage)",
            labels=["fan"],
            unit="percent"
        )

        fan_rpm_gauge = GaugeMetricFamily(
            name="fan_rpm",
            documentation="Fan speed in RPM",
            labels=["fan"]
        )

        fan_config_gauge = GaugeMetricFamily(
            name="fan_config_info",
            documentation="Fan configuration info (profile, governor, control)",
            labels=["fan", "profile", "governor", "control"]
        )

        fan_name = "pwmfan"

        speed = fan_data.get("speed", [0])[0]
        rpm = fan_data.get("rpm", [0])[0]
        profile = fan_data.get("profile", "unknown")
        governor = fan_data.get("governor", "unknown")
        control = fan_data.get("control", "unknown")

        fan_speed_gauge.add_metric([fan_name], speed)
        fan_rpm_gauge.add_metric([fan_name], rpm)
        fan_config_gauge.add_metric([fan_name, profile, governor, control], 1)

        return [fan_speed_gauge, fan_rpm_gauge, fan_config_gauge]

    def __nvpmodel(self):
        from prometheus_client.core import GaugeMetricFamily

        nvpmodel = self.jetson.jtop_stats.get("nvpmodel")
        if not nvpmodel:
            return []

        nvpmodel_gauge = GaugeMetricFamily(
            name="nvpmodel_info",
            documentation="Current NVPModel power mode",
            labels=["model"]
        )

        nvpmodel_gauge.add_metric([str(nvpmodel)], 1)

        return [nvpmodel_gauge]

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

    def __stats(self):
        logging.debug("Starting __stats() method")
        stats_data = self.jetson.jtop_stats.get("stats", {})

        stats_cpu_gauge = GaugeMetricFamily(
            name="stats_cpu_utilization_percent",
            documentation="Per-CPU core utilization or 0 if OFF",
            labels=["core"]
        )

        stats_usage_gauge = GaugeMetricFamily(
            name="stats_resource_usage_percent",
            documentation="Usage percent for RAM, SWAP, GPU, EMC",
            labels=["resource"]
        )

        stats_engine_gauge = GaugeMetricFamily(
            name="stats_engine_status",
            documentation="ON/OFF status for Jetson engines (1=ON, 0=OFF)",
            labels=["engine"]
        )

        stats_fan_gauge = GaugeMetricFamily(
            name="stats_fan_pwm_percent",
            documentation="PWM value for Jetson cooling fans",
            labels=["fan"]
        )

        stats_temp_gauge = GaugeMetricFamily(
            name="stats_temperature_celsius",
            documentation="Temperature sensors from Jetson stats",
            labels=["sensor"],
            unit="celsius"
        )

        stats_power_gauge = GaugeMetricFamily(
            name="stats_power_milliwatts",
            documentation="Per-rail power consumption in milliwatts",
            labels=["rail"],
            unit="mW"
        )

        for key, val in stats_data.items():
            # CPU utilization (CPU1 - CPU12)
            if key.startswith("CPU"):
                core = key[3:]
                try:
                    stats_cpu_gauge.add_metric([core], float(val))
                except ValueError:
                    stats_cpu_gauge.add_metric([core], 0.0)

            # RAM, SWAP, EMC, GPU usage percentages
            elif key in ("RAM", "SWAP", "EMC", "GPU"):
                stats_usage_gauge.add_metric([key], float(val))

            # Engines ON/OFF (exclude jetson_clocks)
            elif key in (
                "APE", "DLA0_CORE", "DLA0_FALCON", "DLA1_CORE", "DLA1_FALCON",
                "NVDEC", "NVENC", "NVJPG", "NVJPG1", "OFA", "PVA0_CPU_AXI",
                "PVA0_VPS", "SE", "VIC"
            ):
                state = 0.0 if val == "OFF" else 1.0
                stats_engine_gauge.add_metric([key], state)

            # Fan PWM
            elif key.startswith("Fan "):
                fan = key[4:].lower().replace(" ", "_")
                stats_fan_gauge.add_metric([fan], float(val))

            # Temperature Sensors
            elif key.startswith("Temp "):
                sensor = key[5:].lower().replace(" ", "_")
                stats_temp_gauge.add_metric([sensor], float(val))

            # Power Rails
            elif key.startswith("Power "):
                rail = key[6:]
                stats_power_gauge.add_metric([rail], float(val))

        return [
            stats_cpu_gauge,
            stats_usage_gauge,
            stats_engine_gauge,
            stats_fan_gauge,
            stats_temp_gauge,
            stats_power_gauge
        ]

    def __processes(self):
        logging.debug("Starting __processes() method")

        processes = self.jetson.jtop_stats.get("processes", [])
        logging.debug(f"Retrieved {len(processes)} processes")

        process_gpu_gauge = GaugeMetricFamily(
            name="process_gpu_usage_percent",
            documentation="Per-process GPU usage percent from Jetson stats",
            labels=["pid", "user", "name"]
        )

        process_mem_gauge = GaugeMetricFamily(
            name="process_memory_usage_kb",
            documentation="Per-process memory usage in KB from Jetson stats",
            labels=["pid", "user", "name"]
        )

        process_rss_gauge = GaugeMetricFamily(
            name="process_memory_rss_kb",
            documentation="Per-process resident memory (RSS) in KB from Jetson stats",
            labels=["pid", "user", "name"]
        )

        for proc in processes:
            try:
                pid = str(proc[0])
                user = str(proc[1])
                name = str(proc[-1])
                gpu = float(proc[6])
                mem = float(proc[7])
                rss = float(proc[8])
            except (IndexError, ValueError, TypeError) as e:
                logging.warning(f"Skipping process row due to parse error: {e}")
                continue

            process_gpu_gauge.add_metric([pid, user, name], gpu)
            process_mem_gauge.add_metric([pid, user, name], mem)
            process_rss_gauge.add_metric([pid, user, name], rss)

        return [
            process_gpu_gauge,
            process_mem_gauge,
            process_rss_gauge
        ]

    def __jetson_clocks(self):
        logging.debug("Starting __jetson_clocks() method")

        clocks_enabled = self.jetson.jtop_stats.get("jetson_clocks", False)
        logging.debug(f"Jetson clocks enabled: {clocks_enabled}")

        clocks_gauge = GaugeMetricFamily(
            name="jetson_clocks",
            documentation="Status of Jetson Clocks override (True if performance mode enabled)",
            labels=["enabled"]
        )

        clocks_gauge.add_metric([str(clocks_enabled).lower()], 1.0)

        return [clocks_gauge]

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
        yield from self.__fan()
        yield from self.__nvpmodel()
        yield from self.__disk()
        yield from self.__uptime()
        yield from self.__stats()
        yield from self.__processes()
        yield from self.__jetson_clocks()
        yield from self.__network_bandwidth()
