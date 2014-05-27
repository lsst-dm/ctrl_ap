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


import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
from lsst.ctrl.ap import jobManager
from lsst.pex.logging import Log

class ArchiveDMCS(object):

    def __init__(self):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "distributor_event"
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "ArchiveDMCS")

    def handleEvents(self):
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        while True:
            self.logger.log(Log.INFO, "listening on %s " % self.eventTopic)
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = ocsEvent.getPropertySet()
            ocsEventType = ps.get("distributor_event")
            self.logger.log(Log.INFO, ocsEventType)
            sequenceTag = ps.get("sequenceTag")
            imageID = ps.get("imageID")
            raft = ps.get("raft")
            inetaddr = ps.get("inetaddr")
            print "sequenceTag = %s, imageID = %s, raft = %s, networkAddress = %s" % (sequenceTag, imageID, raft, inetaddr)

if __name__ == "__main__":
    archive = ArchiveDMCS()
    archive.handleEvents()
