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

class JSONSocket(object):
    def __init__(self, s):
        self.sock = s

    def sendJSON(self, obj):
        s = json.dumps(obj)
        self.sendWithLength(s)

    def recvJSON(self):
        s = self.recvall()
        s = json.loads(s)
        return s

    def sendWithLength(self, s):
        self.sock.sendall(struct.pack('!I',len(s)))
        self.sock.sendall(s)

    def recvall(self):
        # receive a message length and a JSON message
        # the idea here it to get the message length first and then
        # get the rest of the message.  We are never guaranteed to
        # get the full number of bytes requested on a recv, so we go
        # through some extra work to make sure we do, along with not 
        # over reading the amount of data we're supposed to get.
        total = 0
        size = sys.maxint
        recvSize = 4
        dataSize = ""
        data =[]
        while total < size:
            s = self.sock.recv(recvSize)
            if not data:
                # if we haven't put anything in the data buffer yet,
                # we haven't gotten the message length yet, so deal
                # with that first.
                if (len(dataSize)+len(s)) > 4:
                    dataSize += s
                    # this is the actual size of the data we're looking for
                    size = struct.unpack('!I', dataSize[:4])[0]
                    # now use this as the receive size we'd like
                    recvSize=size
                    # in case we got more than 4 bytes (which is likely)
                    # append the rest to the data buffer
                    data.append(dataSize[4:])
                else:
                    dataSize += s
            else:
                # readjust the receive size as we go so we don't overread
                # the socket stream
                recvSize = recvSize-len(s)
                data.append(s)
            total = sum([len(i) for i in data]]
        info = ''.join(data)
        return info
