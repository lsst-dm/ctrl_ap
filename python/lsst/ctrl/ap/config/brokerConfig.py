import lsst.pex.config as pexConfig

class BrokerConfig(pexConfig.Config):
    host = pexConfig.Field("host", dtype=str, default=None)
    port = pexConfig.Field("port", dtype=int, default=61616)
    topic = pexConfig.Field("topic", dtype=str, default=None)
