
class BaseNode:
    def __init__(self, imei, version, monitor, ts, domain, value_range):
        self.imei = imei
        self.version = version
        self.monitor = monitor
        self.ts = ts
        self.domain = domain
        self.value_range = value_range

    def __getitem__(self, item):
        return self.__getattr__(item)

class MonitorParser:
    @staticmethod
    def parse(block_type, path, block):
        return {
            'mem': MonitorParser.parse_process_mem,
            'up_time': MonitorParser.parse_device_up_time,
            'stat': MonitorParser.parse_device_stat,
        }[block_type](path, block)

    @staticmethod
    def parse_process_mem(path, block):
        pass

    @staticmethod
    def parse_device_up_time(path, block):
        pass

    @staticmethod
    def parse_device_stat(path, block):
        pass

    @staticmethod
    def parse_field(path):
        pass

    @staticmethod
    def parse_ts(line):
        pass

