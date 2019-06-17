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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class Crawler {
    final private static NetworkParameters params = MainNetParams.get();

    private static List<InetAddress> visitedPeers = new ArrayList<>();
    private static BlockingQueue<InetSocketAddress> pendingPeers = new LinkedBlockingDeque<>();

    private static InetSocketAddress[] dnsPeers;

    private static final DateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

    public static void main (String[] args) throws Exception {
        Date date = new Date();

        FileWriter fw = new FileWriter("data/addresses_" + sdf.format(date) + ".csv");
        fw.flush();
        fw.write("peer" +  ";" + "address" + "\n");

        FileWriter fw2 = new FileWriter("data/discovered_" + sdf.format(date) + ".csv");
        fw2.flush();
        fw2.write("ip" +  ";" + "port" + ";" + "height" + ";" + "version" + ";" + "subversion" + ";" + "services" + "\n");

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
            InetSocketAddress address = pendingPeers.take();
            final Peer peer = new Peer(params, new VersionMessage(params, 0), null, new PeerAddress(params, address));
            final SettableFuture<Void> future = SettableFuture.create();
            //System.out.println("Connecting to " + address.getAddress().getHostAddress());

            peer.addConnectedEventListener(new PeerConnectedEventListener() {
                @Override
                public void onPeerConnected(Peer p, int peerCount) {
                    VersionMessage ver = peer.getPeerVersionMessage();
                    long bestHeight = ver.bestHeight;
                    int clientVersion = ver.clientVersion;
                    String subVer = ver.subVer;
                    long localServices = ver.localServices;

                    //System.out.println("CONNECTED TO " + peer.getAddress().getAddr().getHostAddress());


                    ListenableFuture<AddressMessage> addrMsgFut = peer.getAddr();

                    Futures.addCallback(addrMsgFut, new FutureCallback<AddressMessage>() {
                        @Override
                        public void onSuccess(@Nullable AddressMessage result) {
                            //System.out.println("Node " + addr + " has sent a addr message.");
                            synchronized (lock) {
                                for (PeerAddress e : result.getAddresses()) {
                                    try {
                                        fw.flush();
                                        fw.write(address.getAddress().getHostAddress() +  ";" + address.getPort() + ";" + e.getAddr().getHostAddress() + ";" + e.getPort() + "\n");
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
                            fw2.write(address.getAddress().getHostAddress() +  ";" + address.getPort() + ";" + bestHeight + ";" + clientVersion + ";" + subVer + ";" + localServices + "\n");
                        } catch (Exception ex) {
                            System.out.println(ex.getMessage());
                        }

                        if (visitedPeers.size() % 50 == 0 || clientManager.getConnectedClientCount() == 0) {
                            System.out.println("Peers visited already: " + visitedPeers.size());
                            //System.out.println("Trying to connect to " + clientManager.getConnectedClientCount() + " peers");
                        }
                        System.out.println("Trying to connect to " + clientManager.getConnectedClientCount() + " peers");

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
        printElapsed(start);
    }

    private static void printElapsed(long start) {
        long now = System.currentTimeMillis();
        System.out.println(String.format("Took %.2f seconds", (now - start) / 1000.0));
    }

}
