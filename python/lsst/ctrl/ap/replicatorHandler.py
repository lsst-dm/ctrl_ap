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
import json
import socket
import threading
import subprocess
import getpass
from lsst.pex.logging import Log
from lsst.ctrl.ap.exceptions import ReplicatorJobException
from lsst.ctrl.ap.exceptions import DistributorException

# this is being deprecated, and will be removed soon.
class ReplicatorHandler(object):
    def __init__(self, jobSocket, distHost, distSock):
        #super(ReplicatorHandler, self).__init__()
        self.jobSock = jobSocket
        self.distHost = distHost
        self.distSock = distSock
        self.logger = Log.getDefaultLog()


    # this thread receives messages from the replicator job and sends
    # those along to the distributor node.
    def go(self):
        # receive message from replicator job
        # this contains information about the exposure #, visit id, and raft
        try:
            s = self.jobSock.recvall()
        except socket.error, err:
            raise ReplicatorJobException("socket error")
        if s == "":
            raise ReplicatorJobException("null string received")
        self.logger.log(Log.INFO, 'received from replicator job %s' % json.loads(s))
        self.logger.log(Log.INFO, 'sending to distributor')

        # send the message straight to the distributor
        # don't need to re-encode it
        try :
            self.distSock.sendWithLength(s)
        except socket.error, err:
            raise DistributorException("error sending message")
        self.logger.log(Log.INFO, 'sent!')

        # the next thing we'll get is a filename from the replicator job.
        # Now, keep in mind that that replicator job runs on the replicator
        # node, so it's already on the machine (in this case on the filesystem).
        vals = self.jobSock.recvJSON()
        msg = vals.copy()
        msg["msgtype"] = "replicator"
        msg["request"] = "upload"

        self.logger.log(Log.INFO, 'name from replicator job %s' % str(name))


        # send the named file to the distributor.
        try:
            self.distSock.sendFile(msg)
        except socket.error, err:
            raise DistributorException("error sending file")
        print "file  %s was sent" % name
