#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008-2012 LSST Corporation.
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

import lsst.pex.config as pexConfig

class DMCSHostConfig(pexConfig.Config):
    """Information about where the base DMCS and failover base DMCS is located
    """
    host = pexConfig.Field(doc="DMCS host name",dtype=str,default=None)
    port = pexConfig.Field(doc="DMCS port number",dtype=int,default=0)

class BaseConfig(pexConfig.Config):
    main = pexConfig.ConfigField("primary base DMCS", BaseHostConfig)
    failover = pexConfig.ConfigField("failover  base DMCS", BaseHostConfig)

class ArchiveConfig(pexConfig.Config):
    main = pexConfig.ConfigField("primary archive DMCS", BaseHostConfig)
    failover = pexConfig.ConfigField("failover archive DMCS", BaseHostConfig)
