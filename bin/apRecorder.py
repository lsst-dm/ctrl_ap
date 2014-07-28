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
import json
import time
import socket
import threading
from lsst.daf.base import PropertySet
import lsst.ctrl.events as events
from lsst.ctrl.ap.status import Status



if __name__ == "__main__":
    eventSystem = events.EventSystem.getDefaultEventSystem()
    eventSystem.createReceiver(Status.broker, Status.topic)

    milliseconds = lambda: int(round(time.time()*1000))

    while True:
        event = eventSystem.receiveEvent(Status.topic)
        milli = milliseconds()
        ps = event.getPropertySet()
        data = ps.get(Status.data)
        values = json.loads(data)
        values["time"] = milli
        data = json.dumps(values)
        print data
        sys.stdout.flush()
