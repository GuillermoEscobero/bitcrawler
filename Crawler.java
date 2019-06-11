package bitcoin.crawler;

import org.bitcoinj.core.*;
import org.bitcoinj.core.listeners.PeerConnectedEventListener;
import org.bitcoinj.core.listeners.PeerDisconnectedEventListener;
import org.bitcoinj.net.NioClientManager;
import org.bitcoinj.net.discovery.DnsDiscovery;
import org.bitcoinj.net.discovery.PeerDiscoveryException;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BriefLogFormatter;

import org.checkerframework.checker.nullness.qual.Nullable;
import com.google.common.util.concurrent.*;

import java.io.FileWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class Crawler {
    final private static NetworkParameters params = MainNetParams.get();

    private static List<InetAddress> visitedPeers = new ArrayList<>();
    private static BlockingQueue<InetSocketAddress> pendingPeers = new LinkedBlockingDeque<>();

    private static InetSocketAddress[] dnsPeers;

    public static void main (String[] args) throws Exception {
        FileWriter fw = new FileWriter("addresses.csv");
        FileWriter fw2 = new FileWriter("discovered.csv");

        Context context = new Context(params); // DO NOT REMOVE

        BriefLogFormatter.init();
        System.out.println("=== Discovering initial peers with DNS ===");
        printDNS();
        System.out.println("===  ======  ======  ======  ======  ======  ===");

        for (InetSocketAddress peer : dnsPeers) pendingPeers.add(peer);
        System.out.println("Scanning " + pendingPeers.size() + " peers:");

        final Object lock = new Object();

        NioClientManager clientManager = new NioClientManager();
        clientManager.startAsync();
        clientManager.awaitRunning();

        while (true) {
            //System.out.println("Peers pending: " + pendingPeers.size());
            //InetSocketAddress addr = pendingPeers.take();

            //InetSocketAddress address = new InetSocketAddress(addr, params.getPort());
            InetSocketAddress address = pendingPeers.take();
            final Peer peer = new Peer(params, new VersionMessage(params, 0), null, new PeerAddress(params, address));
            final SettableFuture<Void> future = SettableFuture.create();

            peer.addConnectedEventListener(new PeerConnectedEventListener() {
                @Override
                public void onPeerConnected(Peer p, int peerCount) {
                    VersionMessage ver = peer.getPeerVersionMessage();
                    long bestHeight = ver.bestHeight;
                    int clientVersion = ver.clientVersion;
                    String subVer = ver.subVer;
                    long localServices = ver.localServices;


                    ListenableFuture<AddressMessage> addrMsgFut = peer.getAddr();

                    Futures.addCallback(addrMsgFut, new FutureCallback<AddressMessage>() {
                        @Override
                        public void onSuccess(@Nullable AddressMessage result) {
                            //System.out.println("Node " + addr + " has sent a addr message.");
                            synchronized (lock) {
                                for (PeerAddress e : result.getAddresses()) {
                                    try {
                                        fw.flush();
                                        fw.write(address.getAddress().getHostAddress() +  ":" + address.getPort() + ";" + e.getAddr().getHostAddress() + ":" + e.getPort() + "\n");
                                    } catch (Exception ex) {
                                        System.out.println(ex.getMessage());
                                    }

                                    if (!visitedPeers.contains(e.getAddr())) {
                                        try {
                                            pendingPeers.put(e.getSocketAddress());
                                        } catch (InterruptedException ex) {
                                            ex.printStackTrace();
                                        }
                                    }
                                }
                            }

                            future.set(null);
                            peer.close();
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            System.out.println("Error on node " + address.getAddress().getHostAddress());
                            future.set(null);
                            peer.close();
                        }
                    }, MoreExecutors.directExecutor());

                    synchronized (lock) {
                        visitedPeers.add(peer.getAddress().getAddr());
                        try {
                            fw2.flush();
                            fw2.write(address.getAddress().getHostAddress() +  ":" + address.getPort() + ";" + bestHeight + ";" + clientVersion + ";" + subVer + ";" + localServices + "\n");
                        } catch (Exception ex) {
                            System.out.println(ex.getMessage());
                        }

                        if (visitedPeers.size() % 50 == 0 || clientManager.getConnectedClientCount() == 0) {
                            System.out.println("Peers visited already: " + visitedPeers.size());
                            System.out.println("Trying to connect to " + clientManager.getConnectedClientCount() + " peers");
                        }
                    }
                }
            });
            peer.addDisconnectedEventListener(new PeerDisconnectedEventListener() {
                @Override
                public void onPeerDisconnected(Peer peer, int peerCount) {
                    if (!future.isDone()) {
                        //System.out.println("Failed to talk to " + addr);
                    } else {
                        //System.out.println("Adios. Remaining: " + clientManager.getConnectedClientCount());
                    }
                    future.set(null);

                }
            });
            clientManager.openConnection(address, peer);
        }

    }

    private static void printDNS() throws PeerDiscoveryException {
        long start = System.currentTimeMillis();
        DnsDiscovery dns = new DnsDiscovery(MainNetParams.get());
        dnsPeers = dns.getPeers(0, 10, TimeUnit.SECONDS);
        //printPeers(dnsPeers);
        printElapsed(start);
    }

//    private static void printPeers(InetSocketAddress[] addresses) {
//        for (InetSocketAddress address : addresses) {
//            String hostAddress = address.getAddress().getHostAddress();
//            System.out.println(String.format("%s:%d", hostAddress, address.getPort()));
//        }
//    }

    private static void printElapsed(long start) {
        long now = System.currentTimeMillis();
        System.out.println(String.format("Took %.2f seconds", (now - start) / 1000.0));
    }

}
