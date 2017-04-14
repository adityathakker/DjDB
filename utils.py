import numpy
import pexpect
import ConfigParser
import os
import utils
import pickle

config = ConfigParser.ConfigParser()
config.read('djdb.cfg')


class WifiLatencyBenchmark(object):
    def __init__(self, ip):
        object.__init__(self)

        self.ip = ip
        self.interval = 0.5

        ping_command = 'ping -i ' + str(self.interval) + ' ' + self.ip
        self.ping = pexpect.spawn(ping_command)

        self.ping.timeout = 1200
        self.ping.readline()  # init
        self.wifi_latency = []
        self.wifi_timeout = 0

    def run_test(self, n_test):
        for n in range(n_test):
            p = self.ping.readline()

            try:
                ping_time = float(p[p.find('time=') + 5:p.find(' ms')])
                self.wifi_latency.append(ping_time)
            except:
                self.wifi_timeout = self.wifi_timeout + 1

        self.wifi_timeout /= float(n_test)
        self.wifi_latency = numpy.array(self.wifi_latency)

    def get_results(self):
        return numpy.mean(self.wifi_latency)


def get_latency(ip):
    my_wifi = WifiLatencyBenchmark(ip)
    my_wifi.run_test(10)
    return my_wifi.get_results()

def update_latency(djdb_dir):
    with open(djdb_dir + "/meta/global.pkl", "wb") as global_meta:
        global_info = dict()
        global_info["nodes"] = dict()
        for key, value in config.items("djdb.clients"):
            ip = value.split(",")[1]
            global_info["nodes"][key] = dict()
            lat = utils.get_latency(ip)
            global_info["nodes"][key]["lat"] = float(lat)

        pickle.dump(global_info, global_meta)

