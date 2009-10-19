# Copyright 2009 Dejan Bosanac <dejan@nighttale.net>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time, threading, sys

import amqplib.client_0_8 as amqp

class PerfRate:

    count = 0
    startTime = time.time()
    
    def printRate(self):
        endTime = time.time()
        period = endTime - self.startTime
        rate  = self.count / (endTime - self.startTime)
        return str(self.count) + " messages in " + str(period) + " secs ; rate=" + str(rate) + " msgs/sec"
    
    def increment(self):
        self.count += 1


class PerfProducer ( threading.Thread ):

    running = True

    def __init__(self, chan):
        self.chan = chan
        self.rate = PerfRate()
        threading.Thread.__init__ ( self )

    def run (self):
        i = 0;
        while (self.running):
            i += 1
            msg = amqp.Message(str(i) + ' Example message')
            msg.properties["delivery_mode"] = 2
            self.chan.basic_publish(msg,exchange="perftest",routing_key="perftest",mandatory=True)
            self.rate.increment()

    def stop(self):
        self.running = False


class ScaleProducer ( threading.Thread ):

    running = True

    def __init__(self, chan):
        self.chan = chan
        self.rate = PerfRate()
        threading.Thread.__init__ ( self )

    def run (self):
        i = 0;
        while (self.running):
            i += 1
            msg = amqp.Message(str(i) + ' Example message')
            msg.properties["delivery_mode"] = 2
            dest = str(i) + "scaletest"
            self.chan.queue_declare(queue=dest, durable=True, exclusive=False, auto_delete=False)
            self.chan.exchange_declare(exchange=dest, type="direct", durable=True, auto_delete=False)
            self.chan.queue_bind(queue=dest, exchange=dest, routing_key=dest)
            self.chan.basic_publish(msg,exchange=dest,routing_key=dest)
            self.rate.increment(s)

    def stop(self):
        self.running = False

class PerfConsumerSync ( threading.Thread ):

    running = True

    def __init__(self, chan):
        self.chan = chan
        self.rate = PerfRate()
        self.chan.basic_consume(queue='perftest', no_ack=False, callback=self.consume, consumer_tag="perftest")
        threading.Thread.__init__ ( self )

    def run (self):
        while (self.running):
            self.chan.wait()

    def consume(self, msg):
        self.rate.increment()
        self.chan.basic_ack(msg.delivery_tag)        


    def stop(self):
        self.running = False
