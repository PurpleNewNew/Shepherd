#!/usr/bin/env python3
"""
Mininet topology skeleton for Shepherd experiments.

This is not meant to be "one click run" for every environment. It is a baseline
to extend with your own link characteristics (tc netem) and bootstrap logic.
"""

from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import OVSController
from mininet.topo import Topo


class StarTopo(Topo):
    def build(self, n_children=4):
        sw = self.addSwitch("s1")
        root = self.addHost("root")
        self.addLink(root, sw)
        for i in range(1, n_children + 1):
            h = self.addHost(f"n{i}")
            self.addLink(h, sw)


def main():
    setLogLevel("info")
    topo = StarTopo(n_children=4)
    net = Mininet(topo=topo, controller=OVSController, link=TCLink, autoSetMacs=True)
    net.start()
    print("\nMininet is up. Use CLI to start shepherd binaries on hosts.\n")
    print("Example:\n")
    print("  mininet> root ./kelpie -s shepherd2026 --ui-grpc-listen 10.0.0.1:50061 --ui-grpc-token reprotoken\n")
    print("  mininet> n1   ./flock  -s shepherd2026 -c 10.0.0.1:40000 -reconnect 1\n")
    CLI(net)
    net.stop()


if __name__ == "__main__":
    main()

