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
import lsst.log as log
from lsst.ctrl.ap.status import Status

class JobManager(object):

    def __init__(self, config):
        self.config = config
        self.temp = None
        self.ads = []

        replicatorSchedulerHost = config.replicator.scheduler
        cRep = htcondor.Collector(replicatorSchedulerHost)
        scheddRep = cRep.locate(htcondor.DaemonTypes.Schedd, replicatorSchedulerHost)
        workerSchedulerHost = config.worker.scheduler
        cWork = htcondor.Collector(workerSchedulerHost)
        scheddWork = cWork.locate(htcondor.DaemonTypes.Schedd, workerSchedulerHost)
        
        # for replicators
        self.repSchedd = htcondor.Schedd(scheddRep)

        # for workers
        self.workerSchedd = htcondor.Schedd(scheddWork)


        ap_dir = os.environ["CTRL_AP_DIR"]
        self.replicatorJobPath = os.path.join(ap_dir,"etc/htcondor/submit/replicator.submit.ad")
        self.wavefrontJobPath = os.path.join(ap_dir,"etc/htcondor/submit/wavefront.submit.ad")
        self.workerJobPath = os.path.join(ap_dir,"etc/htcondor/submit/worker.submit.ad")
        self.wavefrontSensorJobPath = os.path.join(ap_dir,"etc/htcondor/submit/wavefrontSensor.submit.ad")

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

    def submitAllReplicatorJobs(self, visitID, exposureSequenceID):
        status = Status()
        status.publish(status.baseDMCS, status.submit, status.replicatorJobs)
        ad = self.getClassAd(self.replicatorJobPath)

        startingPort = self.config.replicator.startingPort

        # 21 jobs
        raft = 1
        for x in range(0,25):
            if (x == 0) or (x == 4) or (x == 20) or (x == 24):
                continue
            # starting replicatorPort, raft, visitID, exposureSequenceID
            ad["Arguments"] =  "-R %s --raft %s -I %s -x %s" % (str(startingPort), self.encodeToRaft(x), visitID, exposureSequenceID)
            ad["Out"] =  "rep.out.%s.%s" % (str(x), exposureSequenceID)
            ad["Err"] =  "rep.err.%s.%s" % (str(x), exposureSequenceID)
            ad["Log"] =  "rep.log.%s.%s" % (str(x), exposureSequenceID)
            ad["KeepClaimIdle"] =  600
            #ad["ShouldTransferFiles"] =  "NO"
            #ad["WhenToTransferOutput"] =  "ON_EXIT"

            cluster = self.repSchedd.submit(ad,1)
            log.debug("done with this submit")
            raft += 1

        # one job for wavefront
    
        ad["Arguments"] = "-R %s --raft wave -I %s -x %s" % (str(startingPort), visitID, exposureSequenceID)
        ad["Out"] =  "rep.out.%s.%s" % ("wave", exposureSequenceID)
        ad["Err"] =  "rep.err.%s.%s" % ("wave", exposureSequenceID)
        ad["Log"] =  "rep.log.%s.%s" % ("wave", exposureSequenceID)
        ad["KeepClaimIdle"] =  600
        cluster = self.repSchedd.submit(ad,1)
        # TODO: should probably return clusters in a list

    def encodeToRaft(self, raft):
        sRaft = "R:%d,%d" % (raft % 5, raft / 5)
        return sRaft

    def encodeToCcdID(self, ccd):
        sub, raft = self.calculateRaftInfoFromCcd(ccd)
        sRaft = self.encodeToRaft(raft)
        sCcd = "S:%d,%d" % (sub % 3, sub / 3)
        ccdId = "%s\\ %s" % (sRaft, sCcd)
        return ccdId


    # given a ccd number (1 to 189), calculate the raft and the ccd within the
    # raft
    def calculateRaftInfoFromCcd(self, i):
        x = i
        raft = 1
        while (x > 9):
            x = x - 9
            raft += 1
            if (raft == 4) or (raft == 20):
                raft += 1
        return x-1, raft

    def submitWorkerJobs(self, visitID, numExposures, boresightPointing, filterId):
        log.debug("submit worker jobs called")
        status = Status()
        status.publish(status.baseDMCS, status.submit, status.workerJobs)
        ad = self.getClassAd(self.workerJobPath)
        # TODO: change hardcoded archive and port
        archiveHost = "lsst-arch.ncsa.illinois.edu"
        archivePort = 9595
	    # start 189 worker jobs
        for x in range(1,190):
            ccd = self.encodeToCcdID(x)
            sub, raft = self.calculateRaftInfoFromCcd(x)
            sRaft = self.encodeToRaft(raft)
            sCcd = "S:%d,%d" % (sub % 3, sub / 3)
            ad["Arguments"] = "--visitID %s --exposures %s --boresight %s --filterID %s --raft %s --ccd %s" % (visitID, numExposures, boresightPointing, filterId, sRaft, sCcd)
            ad["Out"] =  "worker.Out.%s" % str(x)
            ad["Err"] =  "worker.Err.%s" % str(x)
            ad["Log"] =  "worker.Log.%s" % str(x)
            ad["KeepClaimIdle"] = 600
            #ad["ShouldTransferFiles"] =  "NO"
            #ad["WhenToTransferOutput"] =  "ON_EXIT"
            cluster = self.workerSchedd.submit(ad,1)

        submitWavefronts = False
        if submitWavefronts:
            sensors = ["R:0,0 S:2,2", "R:0,4 S:2,0", "R:4,0 S:0,2", "R:4,4 S:0,0"]
            ad = self.getClassAd(self.wavefrontSensorJobPath)
            for x in range(1,5):
                sensorInfo = sensors[x-1]
                raft = sensorInfo.split(" ")[0]
                sensor = sensorInfo.split(" ")[1]
                ad["Arguments"] = "--visitID %s --exposures %s --boresight %s --filterID %s --raft %s --ccd %s -H %s -P %d" % (visitID, numExposures, boresightPointing, filterId, raft, sensor, archiveHost, archivePort)
                ad["Out"] =  "wavefront.Out.%s" % str(x)
                ad["Err"] =  "wavefront.Err.%s" % str(x)
                ad["Log"] =  "wavefront.Log.%s" % str(x)
                ad["KeepClaimIdle"] = 600
                #ad["ShouldTransferFiles"] =  "NO"
                #ad["WhenToTransferOutput"] =  "ON_EXIT"
                cluster = self.workerSchedd.submit(ad,1)

        # TODO: should probably return clusters in a list

if __name__ == "__main__":
    jm = JobManager()
    for i in range (1,190):
        print jm.encodeToCcdID(i)
