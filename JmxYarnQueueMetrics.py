#!/usr/bin/env python2.7
# Author Jash Lee s905060@gmail.com

import time
import socket
import urllib2
import json


class YarnQueueMetrics():

    def __init__(self):
        self.epoch_time = str(int(time.time()))
        self.metric_prefix = "hadoop.yarn.FairScheduler"
        self.hostname = socket.gethostname()
        self.jmx_endpoint = "http://%s.adnexus.net:8088/jmx" % self.hostname

    def get_jmx(self):
        response = urllib2.urlopen(self.jmx_endpoint)
        data = json.load(response)

        for item in data['beans']:
            modelerType = item['modelerType']

            if "QueueMetrics" in modelerType:
                queues = modelerType.replace("QueueMetrics,", "")
                queue_dict = dict(queue.split("=") for queue in queues.split(","))
                queue = []
                final_queue = ""

                if "user" in queue_dict:
                    for index in xrange(0, len(queue_dict) - 1):
                        queue.append(queue_dict["q" + str(index)])
                    new_queue = "_".join(queue)
                    final_queue = "users." + queue_dict['user'] + '.' + new_queue

                else:
                    for index in xrange(1, len(queue_dict)):
                        queue.append(queue_dict["q" + str(index)])
                    new_queue = "_".join(queue)
                    final_queue = "queues." + new_queue

                for key, value in item.iteritems():
                    exception = ["name", "modelerType", "tag.Hostname", "tag.User"]
                    if key not in exception:
                        print str(self.metric_prefix) + '.' + str(final_queue) + '.' + str(key) + ' ' + str(value) + ' ' + str(self.epoch_time)

if __name__ == "__main__":
    metric_checker = YarnQueueMetrics()
    metric_checker.get_jmx()
