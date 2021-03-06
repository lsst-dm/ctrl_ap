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

class OCS(object):

    def __init__(self, commandTopic="ocs_command", eventTopic="ocs_event"):
        self.commandTopic = commandTopic
        self.eventTopic = eventTopic
        self.brokerName = "lsst8.ncsa.illinois.edu"

    def sendStartIntegration(self, visitID, exposureSequenceID):
        props = PropertySet()
        props.set("ocs_event", "startIntegration")
        props.setString("visitID", visitID)
        props.set("exposureSequenceID", exposureSequenceID)
        print props.toString()

        runId = "ocs"
        event = events.Event(runId, props)

        trans = events.EventTransmitter(self.brokerName, self.eventTopic)
        trans.publishEvent(event)

    def sendStartReadout(self, imageID, visitID, exposureSequenceID):
        props = PropertySet()
        props.set("imageID", imageID)
        props.setString("visitID", visitID)
        props.set("exposureSequenceID", exposureSequenceID)

        runId = "ocs"
        event = events.Event(runId, props)

        trans = events.EventTransmitter(self.brokerName, "ocs_startReadout")
        trans.publishEvent(event)

    def sendNextVisit(self, visitID, exposures, boresight, filterID):
        props = PropertySet()
        props.set("ocs_event", "nextVisit")
        props.set("visitID", visitID)
        props.set("exposures", exposures)
        props.set("boresight", boresight)
        props.set("filterID", filterID)
        print props.toString()
        
        runId = "ocs"
        event = events.Event(runId, props)
        trans = events.EventTransmitter(self.brokerName, self.eventTopic)
        trans.publishEvent(event)
