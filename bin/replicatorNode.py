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
from lsst.ctrl.ap.replicatorHandler import ReplicatorHandler

class ReplicatorNode(Node):

    def __init__(self, port):
        super(ReplicatorNode, self).__init__()
        self.createIncomingSocket(port)


if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-D", "--distributor", type=str, action="store", help="distributor node to connect to", required=True)
    parser.add_argument("-P", "--port", type=int, action="store", help="distributor port to connect to", required=True)
    parser.add_argument("-R", "--replicatorPort", type=int, action="store", help="replicator port for jobs to connect to", required=True)

    args = parser.parse_args()
    rep = ReplicatorNode(args.replicatorPort)
    # connect to the distributor node
    rep.connectToNode(args.distributor, args.port)
    # accept connections from jobs, which send information which needs to
    # be passed on to the distributor
    rep.acceptAndHandle(ReplicatorHandler)
