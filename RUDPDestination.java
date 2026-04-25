import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * RUDPDestination.java — Reliable UDP Receiver (Stop-and-Wait / rdt 3.0)
 * Usage: java RUDPDestination -p <port>
 *
 * Custom Packet Header (10 bytes):
 *   [0]    type   : 0=DATA, 1=ACK, 2=FIN, 3=FILENAME
 *   [1]    seq#   : alternating bit (0 or 1)
 *   [2-5]  offset : byte offset of this chunk in the original file (big-endian int)
 *   [6-9]  dataLen: number of payload bytes following the header (big-endian int)
 *   [10+]  payload: file bytes (DATA) or filename string (FILENAME)
 */
public class RUDPDestination {

    // Packet type constants — must match RUDPSource
    static final byte DATA = 0, ACK = 1, FIN = 2, FILENAME = 3;

    // Fixed header size in bytes
    static final int HDR = 10;

    // main() — parse -p <port>, open the socket, then loop accepting sessions
    public static void main(String[] args) throws Exception {
        // Scan CLI args for "-p <port>"
        int port = -1;
        for (int i = 0; i < args.length - 1; i++)
            if (args[i].equals("-p")) port = Integer.parseInt(args[i + 1]);

        if (port == -1) { System.out.println("Usage: java RUDPDestination -p <port>"); return; }

        DatagramSocket socket = new DatagramSocket(port);
        socket.setSoTimeout(5000); // unblock receive() every 5 s so we can print a waiting message
        System.out.println("[INFO] Listening on port " + port);

        // Accept one file transfer per iteration; loop forever to allow multiple files
        while (true) receiveFile(socket);
    }

    // receiveFile() — handles one complete FILENAME → DATA* → FIN session
    static void receiveFile(DatagramSocket socket) throws Exception {
        byte[]           buf     = new byte[65535]; // max UDP payload buffer
        FileOutputStream fos     = null;            // output file stream
        int              expSeq  = 0;               // expected sequence number (alternating bit)
        long             total   = 0;               // total bytes written to disk
        InetAddress      addr    = null;            // sender's IP (learned on first packet)
        int              srcPort = -1;              // sender's port
        boolean          started = false;           // true once FILENAME has been received
        int             packetCount = 0;         // count of received packets

        while (true) {
            DatagramPacket pkt = new DatagramPacket(buf, buf.length);

            // Block until a packet arrives or the socket times out
            try { socket.receive(pkt); }
            catch (SocketTimeoutException e) {
                // Print a heartbeat if a transfer is in progress, then keep waiting
                if (started) System.out.println("[TIMEOUT] Still waiting for next packet...");
                continue;
            }

            packetCount++; // increment count of received packets

            // Record where to send ACKs
            addr    = pkt.getAddress();
            srcPort = pkt.getPort();
            int len = pkt.getLength();

            // Silently drop packets too small to contain a valid header
            if (len < HDR) continue;

            // ── Parse the 10-byte header
            byte type   = buf[0];
            int  seq    = buf[1] & 0xFF;                       // unsigned byte → int
            int  offset = ByteBuffer.wrap(buf, 2, 4).getInt(); // file offset
            int  dLen   = ByteBuffer.wrap(buf, 6, 4).getInt(); // payload length

            // Sanity-check the declared payload length against actual packet size
            if (dLen < 0 || HDR + dLen > len) { System.out.println("[WARN] Bad packet, discarding."); continue; }

            // Extract the payload bytes
            byte[] payload = Arrays.copyOfRange(buf, HDR, HDR + dLen);

            // ── FILENAME packet — first packet of every transfer
            if (type == FILENAME) {
                String name = new String(payload).trim();
                // Append "-copy" before the extension to avoid overwriting the original
                String out = name.contains(".") ? name.replaceAll("(\\.[^.]+)$", "-copy$1") : name + "-copy";
                fos = new FileOutputStream(out); // create/overwrite the output file
                expSeq = 0; total = 0; started = true;
                System.out.println("[DATA RECEPTION]: start | seq=" + seq + " | FILENAME=" + name + " | OK");
                ack(socket, addr, srcPort, seq); // ACK the filename packet

            // ── FIN packet — sender signals end of file
            } else if (type == FIN) {
                System.out.println("[INFO] FIN received.");
                ack(socket, addr, srcPort, seq); // ACK the FIN so sender can exit
                if (fos != null) { fos.flush(); fos.close(); } // finalize the file
                System.out.println("[COMPLETE] Bytes received: " + total);
                System.out.println("[COMPLETE] Packets received: " + packetCount);
                return; // end this session; caller will loop for the next one

            // ── DATA packet — a chunk of the file
            } else if (type == DATA) {
                if (!started || fos == null) continue; // ignore data before filename

                if (seq == expSeq) {
                    // In-order packet: write to file, advance expected seq, send ACK
                    System.out.println("[DATA RECEPTION]: start | offset=" + offset + " | length=" + dLen + " | OK");
                    fos.write(payload, 0, dLen);
                    total += dLen;
                    expSeq = 1 - expSeq; // flip the alternating bit
                    ack(socket, addr, srcPort, seq);
                    System.out.println("[ACK SENT]: seq=" + seq);
                } else {
                    // Duplicate or out-of-order: discard payload, re-ACK last accepted seq
                    // This unblocks the sender if our previous ACK was lost
                    System.out.println("[DATA RECEPTION]: start | offset=" + offset + " | length=" + dLen + " | DISCARDED");
                    ack(socket, addr, srcPort, 1 - expSeq); // re-ACK the seq we already accepted
                    System.out.println("[ACK RESENT]: seq=" + (1 - expSeq) + " (duplicate discarded)");
                }
            }
        }
    }

    // ack() — send a minimal ACK packet (header only, no payload)
    static void ack(DatagramSocket s, InetAddress addr, int port, int seq) throws Exception {
        byte[] a = new byte[HDR]; // all zeros except type and seq
        a[0] = ACK;
        a[1] = (byte) seq;
        s.send(new DatagramPacket(a, a.length, addr, port));
    }
}