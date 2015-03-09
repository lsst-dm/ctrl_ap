import lsst.pex.config as pexConfig

class HostConfig(pexConfig.Config):
    host = pexConfig.Field("host name", dtype=str, default=None)
    port = pexConfig.Field("host port", dtype=int, default=0)
    heartbeatPort = pexConfig.Field("heartbeat port", dtype=int, default=0)
