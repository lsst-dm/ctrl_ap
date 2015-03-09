import lsst.pex.config as pexConfig

class BrokerConfig(pexConfig.Config):
    host = pexConfig.Field("host", dtype=str, default=None)
    topic = pexConfig.Field("topic", dtype=str, default=None)
