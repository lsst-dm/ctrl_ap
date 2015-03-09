import lsst.pex.config as pexConfig
import fakeTypeMap as fake
from lsst.ctrl.ap.config.brokerConfig import BrokerConfig
from lsst.ctrl.ap.config.hostConfig import HostConfig

class WorkerConfig(pexConfig.Config):
    scheduler = pexConfig.Field("HTCondor scheduler host", dtype=str, default=None)


class ReplicatorConfig(pexConfig.Config):
    scheduler = pexConfig.Field("HTCondor scheduler host", dtype=str, default=None)
    startingPort = pexConfig.Field("Starting port number", dtype=int, default=8000)


class BaseConfig(pexConfig.Config):
    """ Base DMCS configuration information
    """

    broker = pexConfig.ConfigField("event broker information", BrokerConfig)

    main = pexConfig.ConfigField("main base dmcs host information", HostConfig)
    failover = pexConfig.ConfigField("failover base dmcs host information", HostConfig)

    replicator = pexConfig.ConfigField("replicator information", ReplicatorConfig)
    worker = pexConfig.ConfigField("worker information", WorkerConfig)
