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


####
#### this is a prototype and will be refactored
####

improt time

class Monitor(object):

    def __init__(self, host, port):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.host = host
        self.port = port
        self.con = -1

    def connect(self):
        # attempt a connection.

        self.sock = socket.socket(socket.AF_INET, socket,SOCK_STREAM)
        try:
            self.sock.connect((host, port))
        except socket.gaierror, err:
            print "address problem?  %s " % err
            sys.exit(1)
        except socket.error, err:
            print "Connection problem: %s" % err
            return False
        return True

    def checkStatus(self):
        # check status of the socket with a ping/pong message
        return True

        

if __name__ == "__main__":
    # and argparse
    monitor = Monitor(host, port)
    condor = Condot()

    isConnected = monitor.connect()
    if isConnected:
        htcondor.registerFullyOperation()
    else:
        htcondor.registerLocalOnly()

    while True:
        time.sleep(30)
        connected = monitor.checkStatus()
        if connected:
            condor.registerFullyOperation()
        else:
            condor.registerLocalOnly()
