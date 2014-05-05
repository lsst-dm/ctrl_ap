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

import os
import htcondor
import classad

class JobManager(object):

    def __init__(self):
        self.temp = None
        self.ads = []
        self.schedd = htcondor.Schedd() # local schedd
        ap_dir = os.environ["CTRL_AP_DIR"]
        self.replicatorJobPath = os.path.join(ap_dir,"etc/htcondor/submit/replicator.submit.ad")
        self.wavefrontJobPath = os.path.join(ap_dir,"etc/htcondor/submit/broken.submit.ad")

    def getClassAd(self, fileName):
        return classad.parse(open(fileName))
        

    def submitClassAd(self, ad):
        cluster = self.schedd.submit(ad,1)
        return cluster

    def removeJob(self, cluster):
        """
        Remove a job from the queue.
        @param cluster the ClusterId to remove
        """
        # HTCondor expects the name with the ".0" at the end, otherwise
        # it will not be recognized.
        name = "%s.0" % cluster
        values = self.schedd.act(htcondor.JobAction.Remove, [name])
        return values

    def submitReplicatorJob(self, x):
        ad = self.getClassAd(self.replicatorJobPath)
        ad["Arguments"] = str(x)
        cluster = self.schedd.submit(ad,1)
        return cluster

    def submitAllReplicatorJobs(self):
        ad = self.getClassAd(self.replicatorJobPath)
        # should be 21, but we're running 3 for now, because of the test bed
        # we're on
        for x in range(1,4):
            ad["Arguments"] =  "--raft %s -t 1 -x 101" % str(x)
            ad["Out"] =  "/tmp/Out.%s" % str(x)
            ad["Err"] =  "/tmp/Err.%s" % str(x)
            ad["Log"] =  "/tmp/Log.%s" % str(x)
            ad["ShouldTransferFiles"] =  "NO"
            cluster = self.schedd.submit(ad,1)
        ad = self.getClassAd(self.wavefrontJobPath)
        cluster = self.schedd.submit(ad,1)
        # TODO: should probably return clusters in a list

if __name__ == "__main__":
    m = JobManager()
    m.submitAllReplicatorJobs()
