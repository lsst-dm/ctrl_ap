import lsst.pex.config as pexConfig
import fakeTypeMap as fake

class HostConfig(pexConfig.Config):
    host = pexConfig.Field("host name", dtype=str, default=None)
    port = pexConfig.Field("host port", dtype=int, default=0)
    heartbeatPort = pexConfig.Field("heartbeat port", dtype=int, default=0)

class WorkerConfig(pexConfig.Config):
    scheduler = pexConfig.Field("HTCondor scheduler host", dtype=str, default=None)


class ReplicatorHostConfig(pexConfig.Config):
    name = pexConfig.Field("host name", dtype=str, default=None)
    ports = pexConfig.ListField("host ports", dtype=int, default=None)

class ReplicatorConfig(pexConfig.Config):
    scheduler = pexConfig.Field("HTCondor scheduler host", dtype=str, default=None)
    host = pexConfig.ConfigChoiceField("host", fake.FakeTypeMap(ReplicatorHostConfig))


class BaseConfig(pexConfig.Config):
    """ Base DMCS configuration information
    """
    main = pexConfig.ConfigField("main base dmcs host information", HostConfig)
    failover = pexConfig.ConfigField("failover base dmcs host information", HostConfig)

    replicator = pexConfig.ConfigField("replicator information", ReplicatorConfig)
    worker = pexConfig.ConfigField("worker information", WorkerConfig)

    replicatorHostNames = pexConfig.ListField("aliases of replicator hosts", dtype=str)
