import lsst.pex.config as pexConfig
import fakeTypeMap as fake
from lsst.ctrl.ap.config.hostConfig import HostConfig

class ArchiveConfig(pexConfig.Config):
    """ Archive DMCS configuration information
    """

    main = pexConfig.ConfigField("main archive dmcs host information", HostConfig)
    failover = pexConfig.ConfigField("failover archive dmcs host information", HostConfig)
