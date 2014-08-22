#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2014 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import time
import datetime
import os
import sys
import argparse
import socket
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
from lsst.ctrl.ap.job import Job
from lsst.ctrl.ap.node import Node
from lsst.ctrl.ap.status import Status
from lsst.ctrl.ap.replicatorHandler import ReplicatorHandler
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.pex.logging import Log
from lsst.ctrl.ap.exceptions import ReplicatorJobException
from lsst.ctrl.ap.exceptions import DistributorException

from multiprocessing.pool import ThreadPool

class DistributorConnection(Thread):

    def __init__(self, distributor, port, condition, msgList):
        super(DistributorConnection, self).__init__()
        this.distributor = distributor
        this.port = port
        self.msgList = msgList
        this.condition = condition
        this.sleepInterval = 5 # seconds
        self.outSock = None

    def connectToNode(self, host, port):
        outSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # publish status message
        st = Status()
        n = outSock.getsockname()
        name = socket.gethostname()
        serverInfo = {st.host:host, st.port:port}

        connection = {st.server:serverInfo}
        st.publish(st.replicatorNode, st.connect, connection)

        self.logger.log(Log.INFO, "connecting to node %s:%d" % (host, port))
        try:
            outSock.connect((host, port))
            self.outSock = JSONSocket(outSock)
        except socket.gaierror, err:
            self.logger.log(Log.INFO, "address problem?  %s " % err)
            self.outSock = None
        except socket.error, err:
            self.logger.log(Log.INFO, "Connection problem: %s" % err)
            self.outSock = None
        return self.outSock

    def send(self, msg):
        type = msg["msgtype"]
        if type == "file":
            filename = msg["filename"]
            self.outSock.sendFile(filename)
        elif type == "replicator job" or type == "ping":
            self.outSock.sendJSON(msg)
        else:
            print "unknown type: ",type

    def run(self):
        while True:
            if connectionOK == False:
                while self.connectToNode(self.distributor, self.port)
                    time.sleep(self.sleepInterval)
            connectionOK = True
            self.condition.acquire()
            while self.msgList:
                if connectionOK:
                    s = self.msgList[0]
                    try:
                        self.send(s)
                    except socket.error, err:
                        connectionOK = False
                        break
                    msgList.pop(0)
            self.condition.release()
            self.condition.wait()

class ReplicatorJobConnection(Thread):
    def __init__(self, jobSocket, condition, msgList):
        super(ReplicatorJobConnection, self).__init__()
        self.jobSocket = jobSocket
        self.msgList = msgList

    def queueMsg(self, vals)
        self.condition.aquire()
        self.msgList.append(vals)
        self.condition.notifyAll()
        self.condition.release()

    def run(self):
        try:
            vals = self.jobSocket.recvJSON()
        except socket.error, err:
            print "first receive failed; err = ",err
            pass # do interesting something here
        self.queueMsg(vals)
        try:
            vals = self.jobSocket.recvJSON()
        except socket.error, err:
            print "second receive failed; err = ",err
            pass # do interesting something here
        self.queueMsg(vals)
    
        

class ReplicatorNode(Node):

    def __init__(self, distHost, distPort, repPort):
        super(ReplicatorNode, self).__init__()
        self.createIncomingSocket(repPort)
        self.distHost = distHost
        self.distPort = distPort
        self.dSock = None
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "ReplicatorNode")
        self.sleepInterval = 5 # seconds
        st = Status()
        st.publish(st.replicatorNode, st.start, {"server":{"host":socket.gethostname(), "port":repPort}, "distributor":{"host":self.distHost, "port":self.distPort}})

    def activate(self):
        st = Status()
        # connect to distributor

        condition = threading.Condition()
        msgList = list()
        

        dc = DistributorConnection(args.distributor, args.port, condition, msgList)
        dc.start()

        print "replicator loop begun"
        while True:
            print "replicator waiting on accept"
            (clientSock, (ipAddr, clientPort)) = self.inSock.accept()
            print "replicator node: accepted connection"
            client = {"client":{st.host:ipAddr, st.port:clientPort}}
            st.publish(st.replicatorNode, st.accept, client)
            jsock = JSONSocket(clientSock)

            rjc = ReplicatorJobConnection(jsock, condition, msgList)
            rjc.start()

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-D", "--distributor", type=str, action="store", help="distributor node to connect to", required=True)
    parser.add_argument("-P", "--port", type=int, action="store", help="distributor port to connect to", required=True)
    parser.add_argument("-R", "--replicatorPort", type=int, action="store", help="replicator port for jobs to connect to", required=True)

    args = parser.parse_args()
    rep = ReplicatorNode(args.distributor, args.port, args.replicatorPort)
    rep.activate()
