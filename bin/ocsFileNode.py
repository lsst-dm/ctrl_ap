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
import threading
from lsst.ctrl.ap.node import Node
from lsst.ctrl.ap.jsonSocket import JSONSocket

class FileDispatcher(threading.Thread):
    def __init__(self, jsock):
        super(FileDispatcher, self).__init__()
        self.jsock = jsock
        imagePath = os.path.join(os.environ["CTRL_AP_DIR"], "etc", "images")
        self.filename = os.path.join(imagePath, "96x96.png")

    def run(self):
        request = {}

        # receive request for a file from replicator job
        msg = self.jsock.recvJSON()

        request["status"] = "send file"
        request["filename"] = self.filename 

        # send the replicator job the file
        self.jsock.sendFile(request)
        self.jsock.close()
        return

class OCSFileNode(Node):

    def begin(self):

        self.createIncomingSocket(9393)
        while True:
            jsock = self.accept()
            dispatcher = FileDispatcher(jsock)
            dispatcher.start()

if __name__ == "__main__":
    ocs = OCSFileNode()
    print ocs
    ocs.begin()
