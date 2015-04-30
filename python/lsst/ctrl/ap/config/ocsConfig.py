import lsst.pex.config as pexConfig
import fakeTypeMap as fake
from lsst.ctrl.ap.config.brokerConfig import BrokerConfig
from lsst.ctrl.ap.config.hostConfig import HostConfig

class OCSConfig(pexConfig.Config):
    """ OCS configuration information
    """

    broker = pexConfig.ConfigField("event broker information", BrokerConfig)
    commandtopic = pexConfig.Field("topic on which commands are received", dtype=str, default="ocs_command")
