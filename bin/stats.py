#!/usr/bin/env python
import json
import os, sys, argparse
from lsst.ctrl.ap.status import Status

def ocsMessagesSent(content):
    grepFor = {Status.component:Status.ocs, Status.status:Status.sendMsg}
    return grepCount(content, grepFor)

def replicatorJobMessagesReceived(content):
    grepFor = {Status.component:Status.replicatorJob, Status.status:Status.receivedMsg}
    return grepCount(content, grepFor)

def distributorFilesReceived(content):
    grepFor = {Status.component:Status.distributorNode, Status.status:Status.fileReceived}
    return grepCount(content, grepFor)

def distributorFilesSent(content):
    grepFor = {Status.component:Status.distributorNode, Status.status:Status.sendFile}
    return grepCount(content, grepFor)


def workerFilesReceived(content):
    grepFor = {Status.component:Status.workerJob, Status.status:Status.fileReceived}
    return grepCount(content, grepFor)

def mainBaseMessagesReceived(content):
    grepFor = {Status.component:Status.baseDMCS, Status.id:'lsst-base.ncsa.illinois.edu', Status.status:Status.receivedMsg}
    return grepCount(content, grepFor)

def failoverBaseMessagesReceived(content):
    grepFor = {Status.component:Status.baseDMCS, Status.id:'lsst-base2.ncsa.illinois.edu', Status.status:Status.receivedMsg}
    return grepCount(content, grepFor)

def mainArchiveMessagesReceived(content):
    grepFor = {Status.component:Status.archiveDMCS, Status.id:'lsst-arch.ncsa.illinois.edu', Status.status:Status.receivedMsg}
    return grepCount(content, grepFor)

def failoverArchiveMessagesReceived(content):
    grepFor = {Status.component:Status.archiveDMCS, Status.id:'lsst-arch2.ncsa.illinois.edu', Status.status:Status.receivedMsg}
    return grepCount(content, grepFor)

def replicatorJobFileReceived(content):
    grepFor = {Status.status:Status.read, Status.component:Status.replicatorJob}
    return grepCount(content, grepFor)

def wavefrontJobFileReceived(content):
    grepFor = {Status.status:Status.read, Status.component:Status.wavefrontJob}
    return grepCount(content, grepFor)

def replicatorJobUpload(content):
    grepFor = {Status.status:Status.upload, Status.component:Status.replicatorJob}
    return grepCount(content, grepFor)

def wavefrontJobUpload(content):
    grepFor = {Status.status:Status.upload, Status.component:Status.wavefrontJob}
    return grepCount(content, grepFor)

def distributorMessagesToBroker(content):
    grepFor = {Status.status:Status.sendMsg, Status.component:Status.distributorNode}
    return grepCount(content, grepFor)

def grepCount(content, grepFor):
    i = 0
    for x in content:
        s = json.loads(x)
        if contains(s, grepFor):
            i = i + 1
    return i


def contains(s, grepFor):
    for expr in grepFor:
        compareTo = grepFor[expr]
        data = s[expr]
        if not data.startswith(compareTo):
            return False
    return True

if __name__ == "__main__":

    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)

    parser.add_argument("-f", "--file", type=str, action="store", help="file name", required=True)
    parser.add_argument("-V", "--verbose", action="store_true", help="turn on verbosity to see full stat info")

    args = parser.parse_args()


    # ccdSize is full frame, 18bits per pixel, 42% compression/189 ccds
    ccdSize = 17830000 # ((3.56600 gigagbytes)*18bits *.42)/189
    raftSize = ccdSize*9
    imageSize = raftSize*21

    if args.verbose:
        print "calculations based on:"
        print "ccdSize %d bytes " % ccdSize
        print "raftSize %d bytes" % raftSize
        print "imageSize %d bytes" % imageSize
        print


    f = open(args.file)
    content = f.readlines()

    if args.verbose:
        cnt = ocsMessagesSent(content)
        print "Messages sent by OCS broker  =",cnt
        totalSentByOCS = cnt
        print
        cnt = distributorMessagesToBroker(content)
        print "archive DMCS DM messages sent to DM broker =",cnt
        totalToDMBroker = cnt

        cnt = mainArchiveMessagesReceived(content)
        print "main archive DMCS DM messages received from DM broker =",cnt
        totalFromDMBroker = cnt
        cnt = failoverArchiveMessagesReceived(content)
        print "failover archive DMCS DM messages received from DM broker =",cnt
        totalFromDMBroker += cnt
        print
        cnt = replicatorJobFileReceived(content)
        print "replicator job files read from OCS =",cnt
        filesFromOCS = cnt

        cnt = wavefrontJobFileReceived(content)
        print "wavefront job files read from OCS =",cnt
        filesFromOCS += cnt

        cnt = replicatorJobUpload(content)
        print "replicator node files sent to distributor nodes =",cnt
        filesSentToDistributors = cnt

        cnt = wavefrontJobUpload(content)
        print "wavefront node files sent to distributor nodes =",cnt
        filesSentToDistributors += cnt

        cnt = distributorFilesReceived(content)
        print "files received by distributor nodes from replicator nodes =",cnt
        filesReceivedByDistributors = cnt

        cnt = distributorFilesSent(content)
        print "distributor files sent to worker jobs =",cnt
        filesSentToWorkerJobs = cnt
        cnt = workerFilesReceived(content)
        print "worker job files received from distributors =",cnt
        filesReceivedByWorkers = cnt
