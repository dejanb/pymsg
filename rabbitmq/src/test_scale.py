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

import sys, time, threading, perftest

import amqplib.client_0_8 as amqp

conn = amqp.Connection(host="localhost:5672 ", userid="guest", password="guest", virtual_host="/", insist=False)
chan = conn.channel()

producer = perftest.ScaleProducer(chan)
producer.start()

for i in range(1, 1000):
    time.sleep(10)
    print "producer created ", producer.rate.count, " queues"

producer.stop()

chan.close()
conn.close()
