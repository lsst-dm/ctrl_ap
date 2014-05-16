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
from lsst.pex.logging import Log

class DistributorHandler(threading.Thread):
    def __init__(self, sock):
        super(DistributorHandler, self).__init__()
        self.sock = sock
        self.logger = Log.getDefaultLog()

    def run(self):
        while True:
            s = self.sock.recv(1024)
            if s == "":
                self.logger(Log.INFO, 'received nothing')
                return 
            self.logger(Log.INFO, 'received from replicator',s.split(","))
