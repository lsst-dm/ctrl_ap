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

import sys
import socket
from lsst.ctrl.ap.status import Status
from lsst.ctrl.ap.jsonSocket import JSONSocket
import lsst.log as log

class Node(object):

    def __init__(self):
        self.inSock = None
        self.outSock = None


    def createIncomingSocket(self, port):
        host = socket.gethostname()
        inSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        inSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        log.debug("%s: creating incoming socket %s:%d" % (socket.gethostname(), host, port))
        inSock.bind((host, port))
        inSock.listen(5)
        log.debug("done creating socket")
        self.inSock = JSONSocket(inSock)

    def connectToNode(self, component, host, port):
        outSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # publish status message
        st = Status()
        serverInfo = {st.host:host, st.port:port}

        connection = {st.server:serverInfo}
        st.publish(component, st.connect, connection)

        log.debug("connecting to node %s:%d" % (host, port))
        try:
            outSock.connect((host, port))
        except socket.gaierror, err:
            log.warn("address problem?  %s " % err)
            sys.exit(1)
        except socket.error, err:
            log.warn("Connection problem: %s" % err)
            outSock = None
            return False
        self.outSock = JSONSocket(outSock)
        return True

    def accept(self):
        (sock, (ipAddr, clientPort)) = self.inSock.accept()
        return JSONSocket(sock)

    def recvMessage(self):
        return self.inSock.recvJSON()

    def closeSockets(self):
        if self.inSock is not None:
            self.inSock.close()

        if self.outSock is not None:
            self.outSock.close()

    def process(self):
        pass 
