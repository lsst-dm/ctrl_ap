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

class BaseDMCS(object):

    def __init__(self, rHostList):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "ocs_event"
        self.rHostList = rHostList
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "BaseDMCS")

    def handleEvents(self):
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        while True:
            self.logger.log(Log.INFO, "listening on %s " % self.eventTopic)
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = ocsEvent.getPropertySet()
            ocsEventType = ps.get("ocs_event")
            self.logger.log(Log.INFO, ocsEventType)
            if ocsEventType == "startIntegration":
                jm = jobManager.JobManager()
                sequenceTag = ps.get("sequenceTag")
                exposureSequenceID = ps.get("integrationIndex")
                jm.submitAllReplicatorJobs(rHostList, sequenceTag, exposureSequenceID)
            elif ocsEventTYpe == "nextVisit":
                jm = jobManager.JobManager()
                jm.submitWorkerJobs()

if __name__ == "__main__":
    rHostList = []
    # replicator connection locations
    for x in range(1,12):
        rHostList.append(("lsst-rep1.ncsa.illinois.edu", 8000))
        rHostList.append(("lsst-run2.ncsa.illinois.edu", 8000))
        
    base = BaseDMCS(rHostList)
    base.handleEvents()
