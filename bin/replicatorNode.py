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

#def callHandler(jsock, distHost, outSock):
#    rh = ReplicatorHandler(jsock, distHost, outSock)
#    return rh.go()

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

        self.pool = ThreadPool()

    def callHandler(self, jsock, distHost, outSock):
        rh = ReplicatorHandler(jsock, distHost, outSock)
        return rh.go()

    def activate(self):
        st = Status()
        # connect to distributor
        
        needToConnect = True
        needToAccept = True
        while True:
            # repeatedly attempt to connect to distributor, if we can't contact it.
            if needToConnect:
                while self.connectToNode(Status.replicatorNode, args.distributor, args.port) == False:
                    time.sleep(self.sleepInterval)
                self.logger.log(Log.INFO, "connected to distributor Node %s:%d" % (args.distributor, args.port))
                needToConnect = False
            # connection from replicator job
            while True:
                if needToAccept:
                        (clientSock, (ipAddr, clientPort)) = self.inSock.accept()
                        print "replicator node: accepted connection"
                        client = {"client":{st.host:ipAddr, st.port:clientPort}}
                        st.publish(st.replicatorNode, st.accept, client)
                        jsock = JSONSocket(clientSock)
                        needToAccept = False
                    try:
                        async = self.pool.apply_async(self.callHandler, (jsock, self.distHost, self.outSock))
                        result = async.get()
                    except ReplicatorException:
                        needtoAccept = True
                    except DistributorException:
                        needtoConnect = True

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-D", "--distributor", type=str, action="store", help="distributor node to connect to", required=True)
    parser.add_argument("-P", "--port", type=int, action="store", help="distributor port to connect to", required=True)
    parser.add_argument("-R", "--replicatorPort", type=int, action="store", help="replicator port for jobs to connect to", required=True)

    args = parser.parse_args()
    rep = ReplicatorNode(args.distributor, args.port, args.replicatorPort)
    rep.activate()
