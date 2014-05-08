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
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet

class ReplicatorJob(object):

    def __init__(self, raft, expectedSequenceTag, expectedExpSeqID):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "ocs_startReadout"
        self.raft = raft
        self.expectedSequenceTag = expectedSequenceTag
        self.expectedExpSeqID = expectedExpSeqID

    def handleEvents(self):
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        while True:
            ts = time.time()
            print datetime.datetime.fromtimestamp(ts).strftime('listening: %Y-%m-%d %H:%M:%S')
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = ocsEvent.getPropertySet()
            imageID = ps.get("imageID")
            # TODO:  for now, assume visit id, and exp. seq. id is also sent
            sequenceTag = ps.get("sequenceTag")
            exposureSequenceID = ps.get("exposureSequenceID")
            print "image id = %s" % imageID
            print "sequence tag = %s" % sequenceTag
            print "exposure sequence id = %s" % exposureSequenceID
            # NOTE:  While should be done through a selector on the broker
            # so we only get the sequenceTag and exp seq ID we are looking
            # for, DM Messages are not the ultimate way we'll be receiving
            # this info. we'll be using the DDS OCS messages, so this is good
            # for now.
            if sequenceTag == self.expectedSequenceTag and exposureSequenceID == self.expectedExpSeqID:
                print "got expected info.  Getting image"
                self.getCameraImage(imageID, sequenceTag, exposureSequenceID)
                sys.exit(0)

    def getCameraImage(self, imageID, sequenceTag, exposureSequenceID):
        print "getting Camera image for image id = %s, sequenceTag = %s, exposureSequenceID = %s" % (imageID, sequenceTag, exposureSequenceID)

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-r", "--raft", type=int, action="store", help="raft number", required=True)
    parser.add_argument("-t", "--sequenceTag", type=int, action="store", help="sequence Tag", required=True)
    parser.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)

    args = parser.parse_args()
    base = ReplicatorJob(args.raft, args.sequenceTag, args.exposureSequenceID)
    base.handleEvents()
