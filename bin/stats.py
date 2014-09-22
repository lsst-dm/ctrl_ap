#!/usr/bin/env python
import json
import sys

def ocsMessagesSent(content):
    grepFor = {'component':'ocs','status':'send message'}
    return grepCount(content, grepFor)

def replicatorJobMessagesReceived(content):
    grepFor = {'component':'replicator job', 'status':'received message'}
    return grepCount(content, grepFor)

def distributorFiles(content):
    grepFor = {'component':'distributor', 'status':'file received'}
    return grepCount(content, grepFor)

def workerFiles(content):
    grepFor = {'component':'worker job', 'status':'file received'}
    return grepCount(content, grepFor)

def mainBaseMessagesReceived(content):
    grepFor = {'component':'base dmcs', 'componentID':'lsst-base.ncsa.illinois.edu', 'status':'received message'}
    return grepCount(content, grepFor)

def failoverBaseMessagesReceived(content):
    grepFor = {'component':'base dmcs', 'componentID':'lsst-base2.ncsa.illinois.edu', 'status':'received message'}
    return grepCount(content, grepFor)

def mainArchiveMessagesReceived(content):
    grepFor = {'component':'archive dmcs', 'componentID':'lsst-arch.ncsa.illinois.edu', 'status':'received message'}
    return grepCount(content, grepFor)

def failoverArchiveMessagesReceived(content):
    grepFor = {'component':'archive dmcs', 'componentID':'lsst-arch2.ncsa.illinois.edu', 'status':'received message'}
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
    imageSize = 3200000000
    raftSize = imageSize/21
    ccdSize = raftSize/9


    f = open(sys.argv[1])
    content = f.readlines()
    print "Messages sent from OCS =",ocsMessagesSent(content)
    print "replicator job messages received = ",replicatorJobMessagesReceived(content)
    print "main base DMCS messages received =",mainBaseMessagesReceived(content)
    print "failover base DMCS messages received =",failoverBaseMessagesReceived(content)
    print "main archive DMCS messages received from distributors =",mainArchiveMessagesReceived(content)
    print "failover archive DMCS messages received from distributors =",failoverArchiveMessagesReceived(content)

    print "distributor files =",distributorFiles(content)
    print "worker files =",workerFiles(content)
