#!/usr/bin/env python
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from prometheus_client import start_http_server, Gauge
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY
from multiprocessing.dummy import Pool as ThreadPool
import json

MAX_THREADS = 5


def get_hots():
    hosts = {}
    try:
        client = MongoClient('mongodb://mongos1:port1[,mongos2:port2,mongo3:port3...]')
        data = client.admin.command("listShards")
        shard = list()
        for s in data["shards"]:
            tmp = {}
            tmp["name"] = s["_id"]
            tmp["host"] = s["host"].replace(s["_id"] + "/", "").split(",")
            shard.append(tmp)
        hosts["shards"] = shard
        config = client.admin.command("getShardMap")
        hosts["config"] = config["map"]["config"].split(",")
    except Exception as e:
        print e
    finally:
        client.close()

    return hosts


class MongoStatus(object):

    def __init__(self, host):
        self.host = host
        try:
            self.client = MongoClient('mongodb://' + host)
        except Exception as e:
            self.client.close()
            print e

    def server_status(self):
        server_status = {}
        data = self.client.admin.command("serverStatus")
        server_status["connections"] = data["connections"]
        server_status["opcounters"] = data["opcounters"]
        server_status["name"] = self.host
        server_status["network"] = data["network"]
        return server_status

    def rs_status(self):
        result = list()

        try:
            data = self.client.admin.command("replSetGetStatus")
            for member in data["members"]:
                single = {}
                single["name"] = member["name"]
                single["health"] = member["health"]
                single["state"] = member["stateStr"]
                single["set"] = data["set"]
                result.append(single)
        except OperationFailure as e:
            print e
            return False

        return result


class CustomCollector(object):

    def collect(self):
        repl_health = GaugeMetricFamily("mongodb_replset_member_health",
                                        "mongodb replset member health up(1)/down(0)", labels=["name", "set", "state"])

        mongodb_op_counters = CounterMetricFamily(
            "mongodb_op_counters", "mongodb opcounters", labels=["name", "type"])

        mongodb_connections = CounterMetricFamily(
            "mongodb_connections", "mongodb connections", labels=["name", "type"])

        mongodb_network = CounterMetricFamily(
            'mongodb_network', 'mongodb network',labels=["name","type"])

        hosts = get_hots()

        repl = []
        all_hosts = hosts["config"]

        for h in hosts["shards"]:
            repl.append(h["host"][0])
            all_hosts += h["host"]

        data = self.parallel(repl,"rs_status")
        for sub_data in data:
            for d in sub_data:
                repl_health.add_metric(
                    [d["name"], d["set"], d["state"]], int(d["health"]))
        yield repl_health

        server_status = self.parallel(all_hosts,"server_status")

        for s in server_status:
            for k, v in s["connections"].items():
                mongodb_connections.add_metric([s["name"], k], v)

            for k, v in s["opcounters"].items():
                mongodb_op_counters.add_metric([s["name"], k], v)

            for k, v in s["network"].items():
                mongodb_network.add_metric([s["name"], k], v)

        yield mongodb_connections
        yield mongodb_op_counters
        yield mongodb_network

    def parallel(self, hosts,func, threads=MAX_THREADS):
        def tasks_num(host):
            m = MongoStatus(host)
            return getattr(m,func)()

        pool = ThreadPool(threads)
        results = pool.map(tasks_num,hosts)
        pool.close()
        pool.join()
        return results


if __name__ == "__main__":

    from prometheus_client import make_wsgi_app
    from wsgiref.simple_server import make_server
    app = make_wsgi_app(CustomCollector())
    httpd = make_server('', 8000, app)
    httpd.serve_forever()

