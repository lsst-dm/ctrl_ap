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
import struct
import lsst.log as log 

class JSONSocket(object):
    def __init__(self, s):
        self.sock = s

    def sendJSON(self, obj):
        s = json.dumps(obj)
        self.sendWithLength(s)

    def getsockname(self):
        return self.sock.getsockname()

    def _decode_list(self, data):
        rv = []
        for item in data:
            if isinstance(item, unicode):
                item = item.encode("utf-8")
            elif isinstance(item, list):
                item = self._decode_list(item)
            elif isinstance(item, dict):
                item = self._decode_dict(item)
            rv.append(item)
        return rv

    def _decode_dict(self, data):
        rv = {}
        for key, value in data.iteritems():
            if isinstance(key, unicode):
                key = key.encode("utf-8")
            if isinstance(value, unicode):
                value = value.encode("utf-8")
            elif isinstance(value, list):
                value = self._decode_list(value)
            elif isinstance(value, dict):
                value = self._decode_dict(value)
            rv[key] = value
        return rv

    def recvJSON(self):
        s = self.recvall()
        if s == '':
            raise Exception("recv returned empty string")
        rv = json.loads(s, object_hook=self._decode_dict)
        return rv

    def sendFile(self, msg):
        name = msg["filename"]
        log.debug("sendFile: sending file: %s",str(name))

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
        return name

    #def accept(self, level, optname, buflen=None):
    #    # note: we don't use buflen
    #    if buflen is None:
    #        return self.sock.getsockopt(level, optname)
    #    else:
    #        return self.sock.getsockopt(level, optname, buflen)

    def accept(self):
        return self.sock.accept()

    def sendWithLength(self, s):
        self.sock.sendall(struct.pack('!I',len(s)))
        self.sock.sendall(s)

    def getsockopt(self, t, flag):
        return self.sock.getsockopt(t, flag)

    def recv(self, size):
        return self.sock.recv(size)

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
                # connection is either closed, or in the process of being
                # closed.
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

    def close(self):
        self.sock.close()
