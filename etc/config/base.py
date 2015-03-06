root.main.host = "lsst-base.ncsa.illinois.edu"
root.main.port = 9000

root.failover.host = "lsst-base2.ncsa.illinois.edu"
root.failover.port = 9000
root.failover.heartbeatPort = 9090

root.replicatorHostNames = ["host1", "host2"]

root.replicator.scheduler = "lsst-rep.ncsa.illinois.edu"

root.replicator.host["host1"].name = "lsst-rep1.ncsa.illinois.edu"
root.replicator.host["host1"].ports = [8000, 8001, 8002, 8003, 8004, 8005, 8006, 8007, 8008, 8009, 8010]

root.replicator.host["host2"].name = "lsst-rep2.ncsa.illinois.edu"
root.replicator.host["host2"].ports = [8000, 8001, 8002, 8003, 8004, 8005, 8006, 8007, 8008, 8009, 8010]

root.worker.scheduler = "lsst-work.ncsa.illinois.edu"
