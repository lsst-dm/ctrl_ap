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
import htcondor
import classad
from lsst.pex.logging import Log

class JobManager(object):

    def __init__(self):
        self.temp = None
        self.ads = []
        cRep = htcondor.Collector("lsst-rep.ncsa.illinois.edu")
        scheddRep = cRep.locate(htcondor.DaemonTypes.Schedd, "lsst-rep.ncsa.illinois.edu")
        cWork = htcondor.Collector("lsst-work.ncsa.illinois.edu")
        scheddWork = cWork.locate(htcondor.DaemonTypes.Schedd, "lsst-work.ncsa.illinois.edu")
        
        # for replicators
        self.repSchedd = htcondor.Schedd(scheddRep)

        # for workers
        self.workerSchedd = htcondor.Schedd(scheddWork)


        ap_dir = os.environ["CTRL_AP_DIR"]
        self.replicatorJobPath = os.path.join(ap_dir,"etc/htcondor/submit/replicator.submit.ad")
        self.wavefrontJobPath = os.path.join(ap_dir,"etc/htcondor/submit/wavefront.submit.ad")
        self.workerJobPath = os.path.join(ap_dir,"etc/htcondor/submit/worker.submit.ad")
        self.wavefrontSensorJobPath = os.path.join(ap_dir,"etc/htcondor/submit/wavefrontSensor.submit.ad")
        self.logger = Log.getDefaultLog()

    def getClassAd(self, fileName):
        return classad.parse(open(fileName))
        

    def removeJob(self, cluster):
        """
        Remove a job from the queue.
        @param cluster the ClusterId to remove
        """
        # HTCondor expects the name with the ".0" at the end, otherwise
        # it will not be recognized.
        name = "%s.0" % cluster
        values = self.repSchedd.act(htcondor.JobAction.Remove, [name])
        return values

    def submitAllReplicatorJobs(self, rPortList, sequenceTag, exposureSequenceID):
        ad = self.getClassAd(self.replicatorJobPath)
        # 21 jobs
        for x in range(0,25):
            if (x == 0) or (x == 4) or (x == 20) or (x == 24):
                continue
            # replicatorPort, raft, sequenceTag, exposureSequenceID
            entry = rPortList[x]
            rPort = entry[1]
            ad["Arguments"] =  "-R %s --raft %s -t %s -x %s" % (str(rPort), self.encodeToRaft(x), sequenceTag, exposureSequenceID)
            ad["Out"] =  "Out.%s" % str(x)
            ad["Err"] =  "Err.%s" % str(x)
            ad["Log"] =  "Log.%s" % str(x)
            ad["ShouldTransferFiles"] =  "NO"
            ad["WhenToTransferOutput"] =  "ON_EXIT"

            cluster = self.repSchedd.submit(ad,1)
            #self.logger.log(Log.INFO, "done with this submit")

        # one job
        ad = self.getClassAd(self.wavefrontJobPath)
        ad["Arguments"] = "-t %s -x %s" % (sequenceTag, exposureSequenceID)
        cluster = self.repSchedd.submit(ad,1)
        # TODO: should probably return clusters in a list

    def encodeToRaft(self, raft):
        s = "R:%d,%d" % (raft % 5, raft / 5)
        return s

    def submitWorkerJobs(self, visitID, numExposures, boresightPointing, filterId):
        ad = self.getClassAd(self.workerJobPath)
        #for x in range(1,190):
	# start 50 worker jobs
        for x in range(1,51):
            ccd = self.encodeToCCD(x)
            ad["Arguments"] = "--visitID %s --exposures %s --boresight %s --filterID %s --ccd %s" % (visitID, numExposures, boresightPointing, filterId, x)
            cluster = self.workerSchedd.submit(ad,1)

        ad = self.getClassAd(self.wavefrontSensorJobPath)
        for x in range(1,5):
            ad["Arguments"] = "-args %d" % x
            cluster = self.workerSchedd.submit(ad,1)

        # TODO: should probably return clusters in a list
