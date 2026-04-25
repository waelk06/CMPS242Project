import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;

public class RUDPSource {
    static final byte DATA = 0, ACK = 1, FIN = 2, FILENAME = 3;

    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        final int chunkSize = 65500; // max payload bytes per packet
        final int TIMEOUT   = 500;  // ms to wait for an ACK before retransmitting
        byte[] ack  = new byte[10]; // reusable buffer for incoming ACK packets
        int offset  = 0;            // byte offset of the current chunk in the file
        int seqNum  = 0;            // alternating sequence bit (0 or 1)

        if (args.length != 4) {
            // Shows error on wrong number of arguments
            System.out.println("[ERROR] Invalid number of arguments. Correct format: java RUDPSource -r <host>:<port> -f <fileName>");
            return;
        }
            // Extracting the host, port and file name
            String[] host_port = args[1].split(":");
            InetAddress host = InetAddress.getByName(host_port[0]);
            int port = Integer.parseInt(host_port[1]);
            String fileName = args[3];

            try {
                //Reading the file
                FileInputStream fis = new FileInputStream(fileName);

                //Number of bytes to be sent in each packet except the last
                byte[] buffer = new byte[chunkSize];
                DatagramSocket socket = new DatagramSocket();

                // Convert filename to bytes
                byte[] fileNameBytes = fileName.getBytes();

                // Build filename packet
                byte[] fnData = buildPacket(FILENAME, 0, 0, fileNameBytes, fileNameBytes.length);
                DatagramPacket fnPacket = new DatagramPacket(fnData, fnData.length, host, port);

                // Send FILENAME packet and wait for ACK
                boolean fnAckReceived = false;
                while (!fnAckReceived) {
                    socket.send(fnPacket);
                    System.out.println("[FILENAME SENT]: " + fileName);

                    int ackNumberFile= waitForAck(socket,ack,TIMEOUT);
                    if (ackNumberFile == seqNum) {
                        fnAckReceived = true;
                        System.out.println("[FILENAME ACK RECEIVED]");
                    } else if (ackNumberFile == -1) {
                        System.out.println("[TIMEOUT] Retransmitting filename " + fileName);
                    }
                    else{
                        System.out.println("[WRONG ACK RECEIVED]");
                    }

                }
                // Read file in chunks and send each chunk reliably
                int bytesRead;
                int packetCount = 0;
                while ((bytesRead = fis.read(buffer)) != -1 && packetCount < 1000) {

                    // Build data packet
                    byte[] packetData = buildPacket(DATA, seqNum, offset, buffer, bytesRead);
                    DatagramPacket dp = new DatagramPacket(packetData, packetData.length, host, port);

                    boolean ackReceived = false;
                    while (!ackReceived) {
                        long start = System.currentTimeMillis();
                        // Send packet and wait for ACK, retransmit if timeout occurs
                        socket.send(dp);
                        System.out.println("[DATA TRANSMISSION]: " + offset + " | " + bytesRead);
                        int ackNumber = waitForAck(socket,ack, TIMEOUT);

                        if (ackNumber == seqNum) {
                            ackReceived = true;
                            packetCount++;

                            System.out.println("[ACK RECEIVED]: seqNum " + seqNum);
                            long rtt = System.currentTimeMillis() - start;
                            long remaining = 500 - rtt;
                            if (remaining > 0) Thread.sleep(remaining);
                        }
                        else if (ackNumber == -1) {
                            System.out.println("[TIMEOUT] Retransmitting packet seqNum: " + seqNum);
                        }
                        else{
                            System.out.println("[WRONG ACK]: expected " + seqNum + " got " + ackNumber);
                        }

                    }
                    offset += bytesRead;
                    seqNum = 1 - seqNum; // flip the alternating bit
                }
                // Send FIN packet to signal end of transfer
                byte[] endData = buildPacket(FIN, seqNum, 0, null, 0);
                DatagramPacket finPacket = new DatagramPacket(endData, endData.length, host, port);

                // Wait for ACK before sending END to channel
                boolean endAckReceived = false;
                while (!endAckReceived) {
                    socket.send(finPacket);
                    int ackNumber = waitForAck(socket, ack, TIMEOUT);
                    if (ackNumber == seqNum) {
                        endAckReceived = true;
                        System.out.println("[END ACK RECEIVED]");
                    } else if (ackNumber == -1) {
                        System.out.println("[TIMEOUT] Retransmitting FIN signal");
                    } else {
                        System.out.println("[WRONG ACK]: expected " + seqNum + " got " + ackNumber);
                    }
                }

                // Send END string to channel to trigger statistics display
                byte[] endBytes = "END".getBytes();
                DatagramPacket endPacket = new DatagramPacket(endBytes, endBytes.length, host, port);
                socket.send(endPacket);

                System.out.println("[COMPLETE]");

                fis.close();
                socket.close();

            } catch (FileNotFoundException e) {
                System.out.println("[ERROR] File not found: " + fileName);
            } catch (Exception e) {
                System.out.println("[ERROR] Unexpected error: " + e.getMessage());
            }
    }

    // Private method to build the packet
    private static byte[] buildPacket(byte type, int seqNum, int offset, byte[] data, int length){
        ByteBuffer bb = ByteBuffer.allocate(10 + length);

        bb.put(type);
        bb.put((byte)  seqNum);
        bb.putInt(offset);
        bb.putInt(length);

        if (data != null && length > 0) {
            bb.put(data, 0, length);
        }
        return bb.array();
    }

    // Private method waits up to 'timeout' ms for an ACK
    private static int waitForAck(DatagramSocket socket, byte[] ack, int timeout) throws IOException {
        socket.setSoTimeout(timeout);
        DatagramPacket ackPacket = new DatagramPacket(ack, ack.length);
        try {
            java.util.Arrays.fill(ack, (byte) 0);
            socket.receive(ackPacket);
            return ack[1] & 0xFF; // seq number is at byte index 1

        } catch (SocketTimeoutException e) {
            return -1; // signal timeout
        }
    }
}