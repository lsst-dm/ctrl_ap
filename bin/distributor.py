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


import os
import sys
import time
import argparse
import socket
import threading

class DistributorThread(threading.Thread):
    def __init__(self, sock, ip, port):
        self.sock = sock
        self.ip = ip
        self.port = port

    def __run__(self):
        while True:
            if self.handleMessage() == False:
                return

    def handleMessage(self):
        s = self.recv(1024)
        if s == "":
            return False
        print 'received ',s.split(",")
        return True

    def checkStatus(self):
        # check status of the socket with a ping/pong message
        s = self.recv(4)
        print s
        return True

class Distributor(object):

    def __init__(self, port):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.port = port
        self.client = None
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((socket.gethostname(), self.port))
        self.sock.listen(5)

    def acceptAndHandle(self):
        # attempt a connection.

        print "Waiting on connection"
        (self.client, (ipAddr, clientPort)) = self.sock.accept()
        print "connection received: from ip %s:%d" % (ipAddr, clientPort)
        newThread = DistributorThread(self.client, ipAddr, clientPort)
        
        return True


if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])

    parser = argparse.ArgumentParser(prog=basename)
        
    parser.add_argument("-P", "--port", type=int, action="store", help="port to connect to", required=True)

    args = parser.parse_args()

    # add argparse
    distrib = Distributor(args.port)
    while True:
        distrib.acceptAndHandle()
