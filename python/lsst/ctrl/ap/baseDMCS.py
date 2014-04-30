#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

class BaseDMCS(object):

    def __init__(self):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.commandTopic = "ocs_event"


    def handleEvents(self):
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.commandTopic)
        while True:
            ocsEvent = eventSystem.receiveEvent(self.commandTopic)
            ps = event.getPropertySet()
            ocsEventType = ps.get("ocs_event")
            if ocsEventType == "startIntegration":
                job
            

    def sendStartIntegration(self, sequenceTag, integrationIndex):
        eventSystem = events.EventSystem().getDefaultEventSystem()

        originatorId = eventSystem.createOriginatorId()

        props = PropertySet()
        props.set("ocs_event", "startIntegration")
        props.set("sequenceTag", sequenceTag)
        props.set("integrationIndex", integrationIndex)

        runId = "ocs"
        event = events.Event(runId, props)

        trans = events.EventTransmitter(self.brokerName, self.commandTopic)
        trans.publishEvent(event)
