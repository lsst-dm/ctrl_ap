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


import os, sys, struct

import lsst.log as log 

class SocketFileTransfer(object):
    def __init__(self, s):
        self.sock = s

    def send(self, name):
        log.debug("send: sending file: %s", name)

        chunksize = 4096

        # send the size of the file...
        st = os.stat(name)
        size = st.st_size
        self.sock.sendall(struct.pack('!I',size))

        # and the raw data
        chunks = size/chunksize
        leftover = size-chunks*chunksize
        f = open(name)
        for i in range(0,chunks):
            val = f.read(chunksize)
            self.sock.sendall(val)
        val = f.read(leftover)
        self.sock.sendall(val)
        f.close()

    def receive(self, name):
        print "receive: name = ",name
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
        print "receive: all done! = ",name
        return name
