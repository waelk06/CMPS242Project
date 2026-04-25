import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.*;

/**
 * RUDPPeer.java — Reliable UDP Peer (Stop-and-Wait / rdt 3.0)
 * Supports simultaneous send and receive using a dispatcher thread.
 *
 * Usage:
 *   Send + receive: java RUDPPeer -p <listenPort> -r <host>:<port> -f <fileName>
 *   Receive only:   java RUDPPeer -p <listenPort>
 *
 * Architecture:
 *   One shared socket → Dispatcher thread reads every packet and routes it:
 *     ACK packets          → ackQueue   (consumed by sendFile)
 *     DATA/FIN/FILENAME    → dataQueue  (consumed by receiveFile)
 *   This allows both directions to operate simultaneously without
 *   either thread accidentally stealing the other's packets.
 *
 * Packet Header (10 bytes):
 *   [0]    type   : 0=DATA, 1=ACK, 2=FIN, 3=FILENAME
 *   [1]    seq#   : alternating bit (0 or 1)
 *   [2-5]  offset : byte offset of chunk in original file (big-endian int)
 *   [6-9]  dataLen: payload byte count (big-endian int)
 *   [10+]  payload
 */
/*
public class RUDPPeer1 {

    static final byte DATA = 0, ACK = 1, FIN = 2, FILENAME = 3;
    static final int  HDR        = 10;
    static final int  CHUNK_SIZE = 8192;
    static final int  TIMEOUT    = 500;

    // Shared queues — dispatcher routes incoming packets into these
    static final BlockingQueue<byte[]> ackQueue  = new LinkedBlockingQueue<>();
    static final BlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<>();

    // ─────────────────────────────────────────────────────────────────────────
    // main()
    // ─────────────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        int    listenPort = -1;
        String remoteHost = null;
        int    remotePort = -1;
        String fileName   = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p": listenPort = Integer.parseInt(args[++i]); break;
                case "-r":
                    String[] hp = args[++i].split(":");
                    remoteHost = hp[0];
                    remotePort = Integer.parseInt(hp[1]);
                    break;
                case "-f": fileName = args[++i]; break;
            }
        }

        if (listenPort == -1) {
            System.out.println("Usage:");
            System.out.println("  Send+receive: java RUDPPeer -p <listenPort> -r <host>:<port> -f <fileName>");
            System.out.println("  Receive only: java RUDPPeer -p <listenPort>");
            return;
        }

        DatagramSocket socket = new DatagramSocket(listenPort);
        System.out.println("[INFO] Peer listening on port " + listenPort);

        // Start dispatcher — runs for the lifetime of the program
        Thread dispatcher = new Thread(() -> dispatcher(socket));
        dispatcher.setDaemon(true);
        dispatcher.start();

        // Start receiver thread — always running, loops session after session
        Thread receiver = new Thread(() -> {
            while (true) {
                try { receiveFile(socket); }
                catch (Exception e) { System.out.println("[RECEIVER ERROR] " + e.getMessage()); }
            }
        });
        receiver.setDaemon(true);
        receiver.start();

        // If send args provided, send the file on the main thread
        if (remoteHost != null && fileName != null) {
            InetAddress host = InetAddress.getByName(remoteHost);
            sendFile(socket, host, remotePort, fileName);
        }

        // Keep main thread alive so daemon threads keep running
        Thread.currentThread().join();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // dispatcher() — reads every packet off the socket and routes it.
    //
    // For dataQueue packets, the dispatcher prepends 6 bytes of sender metadata
    // so receiveFile() knows where to send ACKs back:
    //   [0-3] = IPv4 address bytes
    //   [4-5] = port as big-endian unsigned short
    //   [6..] = original packet bytes
    // ─────────────────────────────────────────────────────────────────────────
    static void dispatcher(DatagramSocket socket) {
        byte[] buf = new byte[65535];
        while (true) {
            DatagramPacket pkt = new DatagramPacket(buf, buf.length);
            try { socket.receive(pkt); } catch (Exception e) { continue; }

            int    len  = pkt.getLength();
            byte[] data = Arrays.copyOf(pkt.getData(), len);

            if (len < 1) continue;

            if (data[0] == ACK) {
                try { ackQueue.put(data); } catch (InterruptedException ignored) {}
            } else {
                // Prepend sender address + port for receiveFile()
                InetAddress addr      = pkt.getAddress();
                int         port      = pkt.getPort();
                byte[]      addrBytes = addr.getAddress(); // 4 bytes (IPv4)
                byte[]      tagged    = new byte[6 + len];
                System.arraycopy(addrBytes, 0, tagged, 0, 4);
                tagged[4] = (byte) (port >> 8);
                tagged[5] = (byte) (port & 0xFF);
                System.arraycopy(data, 0, tagged, 6, len);
                try { dataQueue.put(tagged); } catch (InterruptedException ignored) {}
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // sendFile() — stop-and-wait sender, reads ACKs from ackQueue
    // ─────────────────────────────────────────────────────────────────────────
    static void sendFile(DatagramSocket socket, InetAddress host, int port, String fileName)
            throws Exception {

        int seqNum = 0;
        int offset = 0;

        FileInputStream fis;
        try {
            fis = new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            System.out.println("[ERROR] File not found: " + fileName);
            return;
        }

        // ── Send FILENAME packet ──────────────────────────────────────────────
        byte[]         fileNameBytes = fileName.getBytes();
        byte[]         fnData        = buildPacket(FILENAME, 0, 0, fileNameBytes, fileNameBytes.length);
        DatagramPacket fnPacket      = new DatagramPacket(fnData, fnData.length, host, port);

        boolean fnAcked = false;
        while (!fnAcked) {
            socket.send(fnPacket);
            System.out.println("[FILENAME SENT]: " + fileName);
            int ack = waitForAck(TIMEOUT);
            if      (ack == 0)  { fnAcked = true; System.out.println("[FILENAME ACK RECEIVED]"); }
            else if (ack == -1) { System.out.println("[TIMEOUT] Retransmitting filename..."); }
            else                { System.out.println("[WRONG ACK] for filename"); }
        }

        // ── Send DATA packets ─────────────────────────────────────────────────
        byte[] buffer = new byte[CHUNK_SIZE];
        int    bytesRead;
        int    packetCount = 0;

        while ((bytesRead = fis.read(buffer)) != -1 && packetCount < 1000) {
            byte[]         pktData = buildPacket(DATA, seqNum, offset, buffer, bytesRead);
            DatagramPacket dp      = new DatagramPacket(pktData, pktData.length, host, port);

            boolean acked = false;
            while (!acked) {
                long start = System.currentTimeMillis();
                socket.send(dp);
                System.out.println("[DATA SENT]: offset=" + offset + " | length=" + bytesRead + " | seq=" + seqNum);

                int ack = waitForAck(TIMEOUT);
                if (ack == seqNum) {
                    acked = true;
                    packetCount++;
                    System.out.println("[ACK RECEIVED]: seq=" + seqNum);
                    long remaining = TIMEOUT - (System.currentTimeMillis() - start);
                    if (remaining > 0) Thread.sleep(remaining);
                } else if (ack == -1) {
                    System.out.println("[TIMEOUT] Retransmitting seq=" + seqNum);
                } else {
                    System.out.println("[WRONG ACK]: expected=" + seqNum + " got=" + ack);
                }
            }

            offset += bytesRead;
            seqNum = 1 - seqNum;
        }

        // ── Send FIN ──────────────────────────────────────────────────────────
        byte[]         finData   = buildPacket(FIN, seqNum, 0, null, 0);
        DatagramPacket finPacket = new DatagramPacket(finData, finData.length, host, port);

        boolean finAcked = false;
        while (!finAcked) {
            socket.send(finPacket);
            int ack = waitForAck(TIMEOUT);
            if      (ack == seqNum) { finAcked = true; System.out.println("[FIN ACK RECEIVED]"); }
            else if (ack == -1)     { System.out.println("[TIMEOUT] Retransmitting FIN..."); }
            else                    { System.out.println("[WRONG ACK] for FIN"); }
        }

        System.out.println("[COMPLETE] Sent " + packetCount + " packets, " + offset + " bytes.");
        fis.close();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // waitForAck() — polls ackQueue with a timeout instead of blocking on socket
    // Returns seq number from the ACK packet, or -1 on timeout
    // ─────────────────────────────────────────────────────────────────────────
    static int waitForAck(int timeoutMs) {
        try {
            byte[] ack = ackQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (ack == null || ack.length < 2) return -1;
            return ack[1] & 0xFF;
        } catch (InterruptedException e) {
            return -1;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // receiveFile() — handles one FILENAME → DATA* → FIN session
    //                 reads tagged packets from dataQueue
    // ─────────────────────────────────────────────────────────────────────────
    static void receiveFile(DatagramSocket socket) throws Exception {
        FileOutputStream fos         = null;
        int              expSeq      = 0;
        long             total       = 0;
        int              packetCount = 0;
        boolean          started     = false;
        InetAddress      addr        = null;
        int              srcPort     = -1;

        while (true) {
            // Wait up to 5 seconds; print heartbeat if mid-transfer
            byte[] tagged = dataQueue.poll(5, TimeUnit.SECONDS);
            if (tagged == null) {
                if (started) System.out.println("[TIMEOUT] Still waiting for next packet...");
                continue;
            }

            // Unpack sender metadata prepended by dispatcher
            byte[] addrBytes = Arrays.copyOfRange(tagged, 0, 4);
            addr    = InetAddress.getByAddress(addrBytes);
            srcPort = ((tagged[4] & 0xFF) << 8) | (tagged[5] & 0xFF);

            // Actual packet starts at byte 6
            byte[] data = Arrays.copyOfRange(tagged, 6, tagged.length);
            int    len  = data.length;

            packetCount++;

            if (len < HDR) continue;

            byte type   = data[0];
            int  seq    = data[1] & 0xFF;
            int  offset = ByteBuffer.wrap(data, 2, 4).getInt();
            int  dLen   = ByteBuffer.wrap(data, 6, 4).getInt();

            if (dLen < 0 || HDR + dLen > len) {
                System.out.println("[WARN] Bad packet, discarding.");
                continue;
            }

            byte[] payload = Arrays.copyOfRange(data, HDR, HDR + dLen);

            // ── FILENAME ──────────────────────────────────────────────────────
            if (type == FILENAME) {
                String name = new String(payload).trim();
                String out  = name.contains(".")
                        ? name.replaceAll("(\\.[^.]+)$", "-copy$1")
                        : name + "-copy";
                fos = new FileOutputStream(out);
                expSeq = 0; total = 0; started = true;
                System.out.println("[FILENAME RECEIVED]: " + name + " → " + out);
                ack(socket, addr, srcPort, seq);

            // ── FIN ───────────────────────────────────────────────────────────
            } else if (type == FIN) {
                System.out.println("[FIN RECEIVED]");
                ack(socket, addr, srcPort, seq);
                if (fos != null) { fos.flush(); fos.close(); }
                System.out.println("[COMPLETE] Bytes received: " + total);
                System.out.println("[COMPLETE] Packets received: " + packetCount);
                return;

            // ── DATA ──────────────────────────────────────────────────────────
            } else if (type == DATA) {
                if (!started || fos == null) continue;

                if (seq == expSeq) {
                    System.out.println("[DATA RECEIVED]: offset=" + offset + " | length=" + dLen + " | seq=" + seq);
                    fos.write(payload, 0, dLen);
                    total  += dLen;
                    expSeq  = 1 - expSeq;
                    ack(socket, addr, srcPort, seq);
                    System.out.println("[ACK SENT]: seq=" + seq);
                } else {
                    System.out.println("[DUPLICATE]: offset=" + offset + " | seq=" + seq + " | DISCARDED");
                    ack(socket, addr, srcPort, 1 - expSeq);
                    System.out.println("[ACK RESENT]: seq=" + (1 - expSeq));
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // buildPacket()
    // ─────────────────────────────────────────────────────────────────────────
    static byte[] buildPacket(byte type, int seqNum, int offset, byte[] data, int length) {
        ByteBuffer bb = ByteBuffer.allocate(HDR + length);
        bb.put(type);
        bb.put((byte) seqNum);
        bb.putInt(offset);
        bb.putInt(length);
        if (data != null && length > 0) bb.put(data, 0, length);
        return bb.array();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ack() — send a minimal 10-byte ACK packet
    // ─────────────────────────────────────────────────────────────────────────
    static void ack(DatagramSocket socket, InetAddress addr, int port, int seq) throws Exception {
        byte[] a = new byte[HDR];
        a[0] = ACK;
        a[1] = (byte) seq;
        socket.send(new DatagramPacket(a, a.length, addr, port));
    }
}
*/