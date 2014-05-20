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
import subprocess
import getpass
from lsst.pex.logging import Log

class ReplicatorHandler(threading.Thread):
    def __init__(self, jobSocket, distHost, distSock):
        super(ReplicatorHandler, self).__init__()
        self.jobSock = jobSocket
        self.distHost = distHost
        self.distSock = distSock
        self.logger = Log.getDefaultLog()
        self.chunksize = 1024


    def sendFile(self, name):
        st = os.path.stat(name)
        size = st.st_size
        chunks = size/self.chunksize
        leftover = size-chunks*self.chunksize
        f = open(name)
        self.distSock.send("{ size : %s }" % size)
        for i in range(0,chunks):
            val = f.read(self.chunksize)
            self.distSock.send(val)
        val = f.read(leftover)
        self.distSock.send(val)
        f.close()

        
    def run(self):
        while True:
            s = self.jobSock.recv(1024)
            if s == "":
                return
            self.logger.log(Log.INFO, 'received from replicator job %s' % s)
            self.logger.log(Log.INFO, 'sending to distributor')
            self.distSock.send(s)
            self.logger.log(Log.INFO, 'sent!')
            name = self.jobSock.recv(1024)
            # TODO: check the file name to transfer
            self.logger.log(Log.INFO, 'received the name = "%s"; sending' % name)

            # send the file
            print "user = %s" % getpass.getuser()
            n = os.path.join("/tmp", os.path.basename(name))
            print "self.distHost = %s" % self.distHost
            print "n = %s" % n
            p = subprocess.Popen(["scp", name, "%s:%s" % (self.distHost,n)], shell=False)
            p.wait()

