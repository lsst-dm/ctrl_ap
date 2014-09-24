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
    mega = 1000000.0

    if args.verbose:
        print "calculations based on:"
        print "ccdSize %.2f megabytes " % (ccdSize/mega)
        print "raftSize %.2f megabytes" % (raftSize/mega)
        print "imageSize %.2f megabytes" % (imageSize/mega)
        print


    f = open(args.file)
    content = f.readlines()

    cnt = ocsMessagesSent(content)
    if args.verbose:
        print "Messages sent by OCS broker  =",cnt
    totalSentByOCS = cnt
    print "Total messages sent by OCS =", totalSentByOCS
    print "Total OCS messages received by components =", totalSentByOCS
    if args.verbose:
        print "(OCS messages are multicast)"
    print


    cnt = distributorMessagesToBroker(content)
    if args.verbose:
        print "distributor messages sent to DM broker =",cnt
    totalToDMBroker = cnt

    cnt = mainArchiveMessagesReceived(content)
    if args.verbose:
        print "main archive DMCS messages received from DM broker =",cnt
    totalFromDMBroker = cnt
    cnt = failoverArchiveMessagesReceived(content)
    if args.verbose:
        print "failover archive DMCS messages received from DM broker =",cnt
    totalFromDMBroker += cnt
    print "total DM messages sent to broker =", totalToDMBroker
    print "total DM messages transmitted by broker = ",totalFromDMBroker
    print

    cnt = replicatorJobFileReceived(content)
    filesFromOCS = cnt
    replicatorJobFileSize = (cnt*raftSize)/mega
    if args.verbose:
        print "replicator job files read from OCS = %d, (%.2f mb)" % (cnt, replicatorJobFileSize)
    filesFromOCSSize = replicatorJobFileSize

    cnt = wavefrontJobFileReceived(content)
    filesFromOCS += cnt
    wavefrontJobFileSize = (cnt*ccdSize*4)/mega
    if args.verbose:
        print "wavefront job files read from OCS = %d, (%.2f mb)" % (cnt, wavefrontJobFileSize)
    filesFromOCSSize += wavefrontJobFileSize
    print "total files from OCS = %d (%.2f mb)" % (filesFromOCS, filesFromOCSSize)
    print

    cnt = replicatorJobUpload(content)
    filesSentToDistributors = cnt
    replicatorJobFileSize = (cnt*raftSize)/mega
    if args.verbose:
        print "replicator job files sent to distributor nodes = %d (%.2f mb)" % (cnt, replicatorJobFileSize)

    cnt = wavefrontJobUpload(content)
    filesSentToDistributors += cnt
    wavefrontJobFileSize = (cnt*ccdSize*4)/mega
    if args.verbose:
        print "wavefront job files sent to distributor nodes = %d (%.2f mb)" % (cnt, wavefrontJobFileSize)

    cnt = distributorFilesReceived(content)
    filesReceivedByDistributors = cnt
    if args.verbose:
        print "files received by distributor nodes from replicator nodes =",cnt

    print "total files received by distributor nodes = %d (%.2f mb)" % (filesReceivedByDistributors, replicatorJobFileSize+wavefrontJobFileSize)

    cnt = distributorFilesSent(content)
    filesSentToWorkerJobs = cnt
    if args.verbose:
        print "distributor files sent to worker jobs =",cnt
    cnt = workerFilesReceived(content)
    filesReceivedByWorkers = cnt
    filesReceivedByWorkersSize = (cnt*ccdSize/mega)
    print "files worker jobs received from distributors %d (%.2f mb)" % (cnt, filesReceivedByWorkersSize)
    print "(wavefront files are not received by worker jobs)"

