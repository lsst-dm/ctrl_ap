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
import sys
import json
import socket
import struct

class JSONSocket(object):
    def __init__(self, s):
        self.sock = s

    def sendJSON(self, obj):
        s = json.dumps(obj)
        #print "sendJSON: s =",s
        self.sendWithLength(s)

    def getsockname(self):
        return self.sock.getsockname()

    def recvJSON(self):
        s = self.recvall()
        #print "recvJSON: '%s'" % s
        #print "type = ",type(s)
        #print "length = ",len(s)
        #print ' '.join(format(ord(x), 'x') for x in s)
        s = json.loads(s)
        #print "recvJSON: '%s'" % s
        return s

    def sendFile(self, msg):
        name = msg["filename"]

        chunksize = 4096

        # send the json info first
        self.sendJSON(msg)

        # then end the size of the file and the raw data
        st = os.stat(name)
        size = st.st_size
        self.sock.sendall(struct.pack('!I',size))

        chunks = size/chunksize
        leftover = size-chunks*chunksize
        f = open(name)
        for i in range(0,chunks):
            val = f.read(chunksize)
            self.sock.sendall(val)
        val = f.read(leftover)
        self.sock.sendall(val)
        f.close()

    # TODO: this is getting refactored, and is temporary
    def recvFile2(self, receiveTo=None):
        vals = self.recvJSON()
        if receiveTo is None:
            name = str(vals["filename"])
        else:
            name = receiveTo
        #print "jsonSocket: recvFile, name = ",name
        return self.recvFile(name)

    def recvFile(self, name):
        total = 0
        size = sys.maxint
        recvSize = 4
        dataSize = ""
        scanningSize = True
        f = open(name,"wb")
        while total < size:
            s = self.sock.recv(recvSize)
            if scanningSize:
                # if we haven't put anything in the data buffer yet,
                # we haven't gotten the message length yet, so deal
                # with that first.
                if (len(dataSize)+len(s)) > 4:
                    dataSize += s
                    # this is the actual size of the data we're looking for
                    size = struct.unpack('!I', dataSize[:4])[0]
                    # now use this as the receive size we'd like
                    # in case we got more than 4 bytes (which is likely)
                    # append the rest to the data buffer
                    f.write(dataSize[4:])
                    total = len(dataSize[4:])
                    recvSize=size-total
                    scanningSize = False
                else:
                    total += len(s)
                    dataSize += s
            else:
                # readjust the receive size as we go so we don't overread
                # the socket stream
                n = len(s)
                total += n
                recvSize = recvSize-n
                f.write(s)
        f.close()
        #print "jsonSocket: recvFile - done"
        return name

    def accept(self):
        return self.sock.accept()

    def sendWithLength(self, s):
        #print "sendWithLength type = ",type(s)
        #print "sendWithLength len = ",len(s)
        self.sock.sendall(struct.pack('!I',len(s)))
        self.sock.sendall(s)

    def recvall(self):
        # receive data which is returned in a buffer
        #
        # the idea here it to get the message length first and then
        # get the rest of the message.  We are never guaranteed to
        # get the full number of bytes requested on a recv, so we go
        # through some extra work to make sure we do, along with not 
        # over reading the amount of data we're supposed to get and
        # accidently spilling into the next message.
        total = 0
        size = sys.maxint
        recvSize = 4
        dataSize = ""
        data =[]
        while total < size:
            s = self.sock.recv(recvSize)
            if s == "":
                # this shouldn't happen in the middle of a message
                return ""
            if not data:
                # if we haven't put anything in the data buffer yet,
                # we haven't gotten the message length yet, so deal
                # with that first.
                if (len(dataSize)+len(s)) > 4:
                    dataSize += s
                    # this is the actual size of the data we're looking for
                    size = struct.unpack('!I', dataSize[:4])[0]
                    # in case we got more than 4 bytes (which is likely)
                    # append the rest to the data buffer
                    data.append(dataSize[4:])
                    # now use this as the receive size we'd like
                    recvSize=size-len(dataSize[4:])
                else:
                    dataSize += s
            else:
                # readjust the receive size as we go so we don't overread
                # the socket stream
                recvSize = recvSize-len(s)
                data.append(s)
            total = sum([len(i) for i in data])
        info = ''.join(data)
        return info
