import java.util.*;
import java.util.concurrent.*;
import java.io.IOException;
import java.net.*;

public class UnreliableChannel {
    //different distribution options for the delay
    private boolean uniformDelay;
    private boolean gaussianDelay;
    private boolean exponentialDelay;

    //min and max for the uniform distribution
    private int minD;
    private int maxD;

    //ports for the channel, the source, and the destination
    public int channelPort;
    public int portA;
    public int portB;

    byte[] receiveBuffer = new byte[2048];

    //mean and stdev for the gaussian distribution
    private double mean;
    private double stdev;

    //lambda for the exponential distribution
    private double lambda;

    //probability of packet loss
    private int lossP;

    //socket and whatnot
    private DatagramSocket socket;

    //multithreaded packet handler that can drop, delay, and send packets
    private void handlePacket(int port, int length, InetAddress addr, byte[] data) {
        if (Math.random() < lossP) {
            return;
        }
        try {
            Thread.sleep(calcDelay());
            if (port == portA) {
                socket.send(new DatagramPacket(data, length, addr, (port == portA ? portB : portA)));
            }
        } catch (Exception e) {
            
        }
    }
    //function for calculating the delay
    public long calcDelay() {
        long delay = 0;

        if (uniformDelay) {
            delay = ThreadLocalRandom.current().nextInt((maxD - minD) + 1) + minD;
        } else if (gaussianDelay) {
            delay = (long) (stdev*ThreadLocalRandom.current().nextGaussian() + mean);
        } else if (exponentialDelay) {
            delay = (long) (-Math.log(ThreadLocalRandom.current().nextDouble())/lambda);
        } 

        return delay;
    }

    //main channel loop, i did this so i can make methods without having to make them static and pass 1 trillion arguments to them
    public void channelStuff() throws IOException {
        ExecutorService pool = Executors.newCachedThreadPool();
        socket = new DatagramSocket(channelPort);
        while (true) {
            DatagramPacket received = new DatagramPacket(receiveBuffer, receiveBuffer.length);
            socket.receive(received);
            InetAddress receivedAddr = received.getAddress();
            int receivedPort = received.getPort();
            int receivedLength = received.getLength();
            byte[] threadBuffer = Arrays.copyOf(received.getData(), receivedLength);
        }
    }
    
    public static void main(String[] args) throws IOException {
        new UnreliableChannel().channelStuff();;
    }
}