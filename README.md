A fork of Exhibitor allowing for the use of Etcd as a config provider.

# Why would you connect one key/value store to another?
There are several projects that have a tightly coupled dependency on ZooKeeper (such as Apache Kaftka). The goal of this fork
is to make it simpler to abstract this dependency by allowing Exhibitor/ZooKeeper instances to be bootstrapped from an Etcd cluster.
