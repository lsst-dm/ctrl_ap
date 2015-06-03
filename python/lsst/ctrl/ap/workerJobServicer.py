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

from lsst.ctrl.ap.status import Status
import lsst.log as log

from sys import getsizeof


class WorkerJobServicer(object):
    def __init__(self, jsock, dataTable, condition):
        log.debug("wrh:  object created")
        self.jsock = jsock
        self.dataTable = dataTable
        self.condition = condition


    def getFileInfo(self, key):
        log.debug("getFileInfo: key = %s", str(key))
        name = None
        self.condition.acquire()
        while True:
            size = getsizeof(self.dataTable)
            size += sum(map(getsizeof,self.dataTable.itervalues())) + sum(map(getsizeof,self.dataTable.iterkeys()));

            log.debug("datatable len = %d size of datatable = %d", len(self.dataTable), size)
            if key in self.dataTable:
                info = self.dataTable[key]
                log.debug("getFileInfo: key = %s", str(key))
                name = info.getName()
                if name is not None: 
                    log.debug("getFileInfo name = %s", str(name))
                    break
                else:
                    log.warn("name was none??")
            else: 
                # worker contacted distributor and the key didn't exist,
                # when it should have.  That means the distributor
                # had previously given this info to the archive DMCS, but
                # the distributor no longer has it (due to reboot, or 
                # expiration.
                log.warn("XXX - key didn't exist")
                self.condition.release()
                return None
            self.condition.wait()
        self.condition.release()
        return name

    def transmitFile(self, key, data):
        log.debug("transmitFile: key = %s", str(key))
        name = self.getFileInfo(key)
        st = Status();
        log.debug("transmitFile requested")
        if name is None:
            # respond to the worker with a message that the file isn't here
            # TODO: tell the archive DMCS to remove that entry
            msg = {st.status:st.error, st.reason:st.fileNotFound}
            self.jsock.sendJSON(msg)
            log.debug("transmitFile NOT sent")
            return
        log.debug("transmitFile: name = %s to %s ", str(name), str(self.jsock.getsockname()))
        msg = data.copy()
        msg["status"] = st.sendFile
        msg["filename"] = name
        log.debug("transmitFile: msg = %s", msg)
        # TODO: tell worker we do have it, and then send file
        self.jsock.sendFile(msg)
        st.publish(st.distributorNode, st.sendFile, {"filename":name})
        log.debug("transmitFile: done; msg = %s", msg)

    def serviceRequest(self, msg):
        request = msg["request"]
        if request == "file":
            visitID = msg["visitID"]
            exposureSequenceID = msg["exposureSequenceID"]
            raft = msg["raft"]
            sensor = msg["sensor"]
            st = Status()
            data = { "visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft, "sensor":sensor}
            st.publish(st.distributorNode, st.requestFile, data)
            key = (visitID, exposureSequenceID, raft, sensor)
            log.debug("transmitting file")
            self.transmitFile(key, data)
            log.debug("file transmitted.  returning")
            return
