package bitcoin.crawler;

import com.google.common.util.concurrent.*;
import org.bitcoinj.core.*;
import org.bitcoinj.core.listeners.PeerConnectedEventListener;
import org.bitcoinj.core.listeners.PeerDisconnectedEventListener;
import org.bitcoinj.net.NioClientManager;
import org.bitcoinj.net.discovery.DnsDiscovery;
import org.bitcoinj.net.discovery.PeerDiscoveryException;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BriefLogFormatter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.FileWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class Crawler {
    final static NetworkParameters params = MainNetParams.get();

    public static List<InetAddress> visitedPeers = new ArrayList<>();
    public static BlockingQueue<InetAddress> pendingPeers = new LinkedBlockingDeque<>();

    private static InetSocketAddress[] dnsPeers;

    public static void main (String[] args) throws Exception {
        FileWriter fw = new FileWriter("visited_test.csv");
        Context context = new Context(params);

        BriefLogFormatter.init();
        System.out.println("=== DNS ===");
        printDNS();
        System.out.println("=== Version/chain heights ===");

        for (InetSocketAddress peer : dnsPeers) pendingPeers.add(peer.getAddress());
        System.out.println("Scanning " + pendingPeers.size() + " peers:");

        final Object lock = new Object();

        List<ListenableFuture<Void>> futures = new ArrayList<>();
        NioClientManager clientManager = new NioClientManager();
        clientManager.startAsync();
        clientManager.awaitRunning();

        while (true) {
            //System.out.println("Peers pending: " + pendingPeers.size());
            InetAddress addr = pendingPeers.take();
            InetSocketAddress address = new InetSocketAddress(addr, params.getPort());
            final Peer peer = new Peer(params, new VersionMessage(params, 0), null, new PeerAddress(params, address));
            final SettableFuture<Void> future = SettableFuture.create();

            peer.addConnectedEventListener(new PeerConnectedEventListener() {
                @Override
                public void onPeerConnected(Peer p, int peerCount) {
                    ListenableFuture<AddressMessage> addrMsgFut = peer.getAddr();

                    Futures.addCallback(addrMsgFut, new FutureCallback<AddressMessage>() {
                        @Override
                        public void onSuccess(@Nullable AddressMessage result) {
                            //System.out.println("Node " + addr + " has sent a addr message.");
                            synchronized (lock) {
                                for (PeerAddress e : result.getAddresses()) {
                                    try {
                                        fw.flush();
                                        fw.write(addr.getHostAddress() + ";" + e.getAddr().getHostAddress() + "\n");
                                    } catch (Exception ex) {
                                        System.out.println(ex.getMessage());
                                    }

                                    if (!visitedPeers.contains(e.getAddr())) {
                                        try {
                                            pendingPeers.put(e.getAddr());
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
                            System.out.println("Error on node " + addr);
                            future.set(null);
                            peer.close();
                        }
                    }, MoreExecutors.directExecutor());

                    synchronized (lock) {
                        visitedPeers.add(peer.getAddress().getAddr());
                        if (visitedPeers.size() % 50 == 0) {
                            System.out.println("Peers visited already: " + visitedPeers.size());
                            System.out.println("Trying to connect to " + clientManager.getConnectedClientCount());
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
            futures.add(future);
        }

        //Futures.successfulAsList(futures).get();
        //System.out.println("END");
    }

    private static void printDNS() throws PeerDiscoveryException {
        long start = System.currentTimeMillis();
        DnsDiscovery dns = new DnsDiscovery(MainNetParams.get());
        dnsPeers = dns.getPeers(0, 10, TimeUnit.SECONDS);
        printPeers(dnsPeers);
        printElapsed(start);
    }

    private static void printPeers(InetSocketAddress[] addresses) {
        for (InetSocketAddress address : addresses) {
            String hostAddress = address.getAddress().getHostAddress();
            System.out.println(String.format("%s:%d", hostAddress, address.getPort()));
        }
    }

    private static void printElapsed(long start) {
        long now = System.currentTimeMillis();
        System.out.println(String.format("Took %.2f seconds", (now - start) / 1000.0));
    }

}
