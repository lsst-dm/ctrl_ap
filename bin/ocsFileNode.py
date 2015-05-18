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
from lsst.ctrl.ap.node import Node
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.ctrl.ap.status import Status

class OCSFileNode(Node):
    def __init__(self):
        imagePath = os.path.join(os.environ["CTRL_AP_DIR"], "etc", "images")
        self.filename = os.path.join(imagePath, "96x96.png")

    def begin(self):
        request = {}
        st = Status()

        self.createIncomingSocket(9393)
        while True:
            jsock = self.accept()
            msg = jsock.recvJSON()
            print msg
            # to do: send correct raft based on request
            request["status"] = st.sendFile
            request["filename"] = self.filename 
            jsock.sendFile(request)
            jsock.close()
            jsock = None

if __name__ == "__main__":
    ocs = OCSFileNode()
    print ocs
    ocs.begin()
