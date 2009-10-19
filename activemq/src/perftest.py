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

import threading, time, pyactivemq, sys, signal

from pyactivemq import ActiveMQConnectionFactory, DeliveryMode


def clean(*args):
    print "the end!"
    sys.exit(0)

for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, clean)

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
        
        
class PerfListener(pyactivemq.MessageListener):
    
    def __init__(self, rate):
        pyactivemq.MessageListener.__init__(self)
        self.rate = rate
    
    def onMessage(self, message):
        self.rate.increment()

class PerfConsumerAsync:


    def __init__(self, factory, destination):
        self.factory = factory
        self.destination = destination
        self.conn = self.factory.createConnection()
        self.session = self.conn.createSession()
        self.consumer = self.session.createConsumer(self.destination)
        self.rate = PerfRate()
        self.consumer.messageListener = PerfListener(self.rate)    



    def start(self):
        self.conn.start()

    def stop(self):
        self.conn.stop()

    def shutdown(self):
        self.conn.close()

class PerfConsumerSync ( threading.Thread ):

    running = True

    def __init__(self, factory, destination):
        self.factory = factory
        self.destination = destination
        self.conn = self.factory.createConnection()
        self.session = self.conn.createSession()
        self.consumer = self.session.createConsumer(self.destination)
        self.rate = PerfRate()
        threading.Thread.__init__ ( self )

    def run (self):
        while (self.running):
            textMessage = self.consumer.receive()
            if (textMessage != None):
                self.rate.increment()

    def stop(self):
        self.running = False
        self.conn.stop()

    def shutdown(self):
        self.conn.close()

    def start(self):
        self.conn.start()
        threading.Thread.start(self)


class PerfProducer ( threading.Thread ):

    running = True

    def __init__(self, factory, destination):
        self.factory = factory
        self.destination = destination
        self.conn = self.factory.createConnection()
        self.conn.start()
        self.session = self.conn.createSession()
        self.producer = self.session.createProducer(self.destination)
        #self.producer.deliveryMode = DeliveryMode.NON_PERSISTENT
        self.rate = PerfRate()
        threading.Thread.__init__ ( self )

    def run (self):
        i = 0;
        while (self.running):
            textMessage = self.session.createTextMessage()
            i += 1
            textMessage.text = str(i) + ' Example message'
            self.producer.send(textMessage)
            self.rate.increment()

    def stop(self):
        self.running = False
        self.conn.stop()

    def shutdown(self):
        self.conn.close()

class ScaleProducer ( threading.Thread ):

    running = True

    def __init__(self, factory):
        self.factory = factory
        self.conn = self.factory.createConnection()
        self.conn.start()
        self.session = self.conn.createSession()
        self.rate = PerfRate()
        threading.Thread.__init__ ( self )

    def run (self):
        i = 0;
        while (self.running):
            textMessage = self.session.createTextMessage()
            i += 1
            textMessage.text = str(i) + ' Example message'
            destName = str(i) + "scaletest"
            producer = self.session.createProducer(self.session.createQueue(destName))
            producer.send(textMessage)
            producer.close()
            self.rate.increment()

    def stop(self):
        self.running = False
        self.conn.stop()

    def shutdown(self):
        self.conn.close()

