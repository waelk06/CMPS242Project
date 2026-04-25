import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * RUDPPeer.java — Reliable UDP Peer (Stop-and-Wait / rdt 3.0)
 * Each peer can both send and receive files.
 *
 * Usage:
 *   Send mode:    java RUDPPeer -p <listenPort> -r <host>:<port> -f <fileName>
 *   Receive mode: java RUDPPeer -p <listenPort>
 *
 * Custom Packet Header (10 bytes):
 *   [0]    type   : 0=DATA, 1=ACK, 2=FIN, 3=FILENAME
 *   [1]    seq#   : alternating bit (0 or 1)
 *   [2-5]  offset : byte offset of this chunk in the original file (big-endian int)
 *   [6-9]  dataLen: number of payload bytes following the header (big-endian int)
 *   [10+]  payload: file bytes (DATA) or filename string (FILENAME)
 */
public class RUDPPeer {

    // Packet type constants
    static final byte DATA = 0, ACK = 1, FIN = 2, FILENAME = 3;

    // Fixed header size in bytes
    static final int HDR = 10;

    // Max payload bytes per DATA packet
    static final int CHUNK_SIZE = 60000;

    // ACK timeout in milliseconds
    static final int TIMEOUT = 500;

    // ─────────────────────────────────────────────────────────────────────────
    // main() — parse args and decide send vs receive mode
    // ─────────────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        int    listenPort = -1;
        String remoteHost = null;
        int    remotePort = -1;
        String fileName   = null;

        // Parse all flags
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p":
                    listenPort = Integer.parseInt(args[++i]);
                    break;
                case "-r":
                    String[] hp = args[++i].split(":");
                    remoteHost = hp[0];
                    remotePort = Integer.parseInt(hp[1]);
                    break;
                case "-f":
                    fileName = args[++i];
                    break;
            }
        }

        if (listenPort == -1) {
            System.out.println("Usage:");
            System.out.println("  Send mode:    java RUDPPeer -p <listenPort> -r <host>:<port> -f <fileName>");
            System.out.println("  Receive mode: java RUDPPeer -p <listenPort>");
            return;
        }

        // Open a single socket bound to the listen port — used for both sending and receiving
        DatagramSocket socket = new DatagramSocket(listenPort);
        System.out.println("[INFO] Peer listening on port " + listenPort);

        if (remoteHost != null && fileName != null) {
            // ── SEND MODE: send a file, then stay alive to receive files too ──
            InetAddress host = InetAddress.getByName(remoteHost);
            sendFile(socket, host, remotePort, fileName);

            // After sending, drop into receive loop so this peer can also accept files
            System.out.println("[INFO] Send complete. Now listening for incoming files...");
            socket.setSoTimeout(5000);
            while (true) receiveFile(socket);
        } else {
            // ── RECEIVE MODE: just listen forever ────────────────────────────
            socket.setSoTimeout(5000);
            while (true) receiveFile(socket);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // sendFile() — reliably sends a file using stop-and-wait (rdt 3.0)
    // ─────────────────────────────────────────────────────────────────────────
    static void sendFile(DatagramSocket socket, InetAddress host, int port, String fileName)
            throws Exception {

        byte[] ackBuf  = new byte[HDR];
        int    seqNum  = 0;
        int    offset  = 0;

        FileInputStream fis;
        try {
            fis = new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            System.out.println("[ERROR] File not found: " + fileName);
            return;
        }

        // ── Send FILENAME packet ──────────────────────────────────────────────
        byte[] fileNameBytes = fileName.getBytes();
        byte[] fnData        = buildPacket(FILENAME, 0, 0, fileNameBytes, fileNameBytes.length);
        DatagramPacket fnPacket = new DatagramPacket(fnData, fnData.length, host, port);

        boolean fnAckReceived = false;
        while (!fnAckReceived) {
            socket.send(fnPacket);
            System.out.println("[FILENAME SENT]: " + fileName);
            int ackSeq = waitForAck(socket, ackBuf, TIMEOUT);
            if (ackSeq == 0) {           // FILENAME always uses seq=0
                fnAckReceived = true;
                System.out.println("[FILENAME ACK RECEIVED]");
            } else if (ackSeq == -1) {
                System.out.println("[TIMEOUT] Retransmitting filename...");
            } else {
                System.out.println("[WRONG ACK] for filename");
            }
        }

        // ── Send DATA packets ─────────────────────────────────────────────────
        byte[] buffer     = new byte[CHUNK_SIZE];
        int    bytesRead;
        int    packetCount = 0;

        while ((bytesRead = fis.read(buffer)) != -1 && packetCount < 1000) {
            byte[]         packetData = buildPacket(DATA, seqNum, offset, buffer, bytesRead);
            DatagramPacket dp         = new DatagramPacket(packetData, packetData.length, host, port);

            boolean ackReceived = false;
            while (!ackReceived) {
                long start = System.currentTimeMillis();
                socket.send(dp);
                System.out.println("[DATA SENT]: offset=" + offset + " | length=" + bytesRead + " | seq=" + seqNum);

                int ackSeq = waitForAck(socket, ackBuf, TIMEOUT);
                if (ackSeq == seqNum) {
                    ackReceived = true;
                    packetCount++;
                    System.out.println("[ACK RECEIVED]: seq=" + seqNum);

                    // Rate limiting: wait out the remainder of the 500ms window
                    long remaining = TIMEOUT - (System.currentTimeMillis() - start);
                    if (remaining > 0) Thread.sleep(remaining);

                } else if (ackSeq == -1) {
                    System.out.println("[TIMEOUT] Retransmitting seq=" + seqNum);
                } else {
                    System.out.println("[WRONG ACK]: expected=" + seqNum + " got=" + ackSeq);
                }
            }

            offset += bytesRead;
            seqNum = 1 - seqNum; // flip alternating bit
        }

        // ── Send FIN packet ───────────────────────────────────────────────────
        byte[]         finData   = buildPacket(FIN, seqNum, 0, null, 0);
        DatagramPacket finPacket = new DatagramPacket(finData, finData.length, host, port);

        boolean finAckReceived = false;
        while (!finAckReceived) {
            socket.send(finPacket);
            int ackSeq = waitForAck(socket, ackBuf, TIMEOUT);
            if (ackSeq == seqNum) {
                finAckReceived = true;
                System.out.println("[FIN ACK RECEIVED]");
            } else if (ackSeq == -1) {
                System.out.println("[TIMEOUT] Retransmitting FIN...");
            } else {
                System.out.println("[WRONG ACK] for FIN");
            }
        }

        // Signal the channel/receiver that the transfer is done
        byte[]         endBytes  = "END".getBytes();
        DatagramPacket endPacket = new DatagramPacket(endBytes, endBytes.length, host, port);
        socket.send(endPacket);

        System.out.println("[COMPLETE] Sent " + packetCount + " packets, " + offset + " bytes.");
        fis.close();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // receiveFile() — handles one complete FILENAME → DATA* → FIN session
    // ─────────────────────────────────────────────────────────────────────────
    static void receiveFile(DatagramSocket socket) throws Exception {
        byte[]           buf        = new byte[65535];
        FileOutputStream fos        = null;
        int              expSeq     = 0;
        long             total      = 0;
        int              packetCount = 0;
        InetAddress      addr       = null;
        int              srcPort    = -1;
        boolean          started    = false;

        while (true) {
            DatagramPacket pkt = new DatagramPacket(buf, buf.length);

            try { socket.receive(pkt); }
            catch (SocketTimeoutException e) {
                if (started) System.out.println("[TIMEOUT] Still waiting for next packet...");
                continue;
            }

            packetCount++;
            addr    = pkt.getAddress();
            srcPort = pkt.getPort();
            int len = pkt.getLength();

            // Drop packets too small for a valid header
            if (len < HDR) continue;

            // Parse header
            byte type   = buf[0];
            int  seq    = buf[1] & 0xFF;
            int  offset = ByteBuffer.wrap(buf, 2, 4).getInt();
            int  dLen   = ByteBuffer.wrap(buf, 6, 4).getInt();

            if (dLen < 0 || HDR + dLen > len) {
                System.out.println("[WARN] Bad packet, discarding.");
                continue;
            }

            byte[] payload = Arrays.copyOfRange(buf, HDR, HDR + dLen);

            // ── FILENAME ──────────────────────────────────────────────────────
            if (type == FILENAME) {
                String name = new String(payload).trim();
                String out  = name.contains(".")
                        ? name.replaceAll("(\\.[^.]+)$", "-copy$1")
                        : name + "-copy";
                fos     = new FileOutputStream(out);
                expSeq  = 0; total = 0; started = true;
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
                    System.out.println("[DATA RECEIVED]: offset=" + offset + " | length=" + dLen + " | seq=" + seq + " | OK");
                    fos.write(payload, 0, dLen);
                    total  += dLen;
                    expSeq  = 1 - expSeq;
                    ack(socket, addr, srcPort, seq);
                    System.out.println("[ACK SENT]: seq=" + seq);
                } else {
                    System.out.println("[DUPLICATE]: offset=" + offset + " | seq=" + seq + " | DISCARDED");
                    ack(socket, addr, srcPort, 1 - expSeq); // re-ACK last accepted
                    System.out.println("[ACK RESENT]: seq=" + (1 - expSeq));
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // buildPacket() — assemble a header + payload into a byte array
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
    // waitForAck() — block up to 'timeout' ms for an ACK, return seq or -1
    // ─────────────────────────────────────────────────────────────────────────
    static int waitForAck(DatagramSocket socket, byte[] ackBuf, int timeout) throws IOException {
        socket.setSoTimeout(timeout);
        DatagramPacket ackPacket = new DatagramPacket(ackBuf, ackBuf.length);
        try {
            Arrays.fill(ackBuf, (byte) 0);
            socket.receive(ackPacket);
            return ackBuf[1] & 0xFF;
        } catch (SocketTimeoutException e) {
            return -1;
        }
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