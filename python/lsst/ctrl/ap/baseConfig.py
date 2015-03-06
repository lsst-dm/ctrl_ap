import lsst.pex.config as pexConfig
import fakeTypeMap as fake

class HostConfig(pexConfig.Config):
    host = pexConfig.Field("host name", dtype=str, default=None)
    port = pexConfig.Field("host port", dtype=int, default=0)
    heartbeatPort = pexConfig.Field("heartbeat port", dtype=int, default=0)

class WorkerConfig(pexConfig.Config):
    scheduler = pexConfig.Field("HTCondor scheduler host", dtype=str, default=None)


class ReplicatorConfig(pexConfig.Config):
    scheduler = pexConfig.Field("HTCondor scheduler host", dtype=str, default=None)
    startingPort = pexConfig.Field("Starting port number", dtype=int, default=8000)


class BaseConfig(pexConfig.Config):
    """ Base DMCS configuration information
    """
    main = pexConfig.ConfigField("main base dmcs host information", HostConfig)
    failover = pexConfig.ConfigField("failover base dmcs host information", HostConfig)

    replicator = pexConfig.ConfigField("replicator information", ReplicatorConfig)
    worker = pexConfig.ConfigField("worker information", WorkerConfig)
