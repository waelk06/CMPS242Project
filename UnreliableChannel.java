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

    //socket and whatnot
    private DatagramSocket socket;

    //gilbert model stuff
    private BurstLossSimulator burst;

    //constructor 
    public UnreliableChannel(char distribution, int minD, int maxD, double mean, double stdev, double lambda, int channelPort, int portA, int portB, double goodP, double burstP, double goodToBurst, double burstToGood) {
        switch (distribution) {
            case 'u':
                uniformDelay = true;
                gaussianDelay = false;
                exponentialDelay = false;
                this.minD = minD;
                this.maxD = maxD;
                break;
            case 'g':
                uniformDelay = false;
                gaussianDelay = true;
                exponentialDelay = false;
                this.mean = mean;
                this.stdev = stdev;
                break;
            case 'e':
                uniformDelay = false;
                gaussianDelay = false;
                exponentialDelay = true;
                this.lambda = lambda;
            default:
                uniformDelay = true;
                gaussianDelay = false;
                exponentialDelay = false;
                minD = 0;
                maxD = 600;
                break;
        }
        this.burst = new BurstLossSimulator(goodP, burstP, goodToBurst, burstToGood);
        this.channelPort = channelPort;
        this.portA = portA;
        this.portB = portB;
    }

    //multithreaded packet handler that can drop, delay, and send packets
    private void handlePacket(int port, int length, InetAddress addr, byte[] data) {
        if (burst.drop()) {
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
            pool.submit(() -> handlePacket(receivedPort, receivedLength, receivedAddr, threadBuffer));
        }
    }
    
    public static void main(String[] args) throws Exception {
        double goodP = 0;
        double burstP = 0;
        double goodToBurst = 0;
        double burstToGood = 0;
        int minD = 0;
        int maxD = 600;
        char distribution = ' ';
        double mean = 0;
        double stdev = 1;
        double lambda = 0;
        int channelPort = 0;
        int portA = 0;
        int portB = 0;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-ports":
                        channelPort = Integer.parseInt(args[++i]);
                        portA = Integer.parseInt(args[++i]);
                        portB = Integer.parseInt(args[++i]);
                        break;
                    case "-p":
                        goodP = Double.parseDouble(args[++i]);
                        burstP = Double.parseDouble(args[++i]);
                        goodToBurst = Double.parseDouble(args[++i]);
                        burstToGood = Double.parseDouble(args[++i]);
                        break;
                    case "-u":
                        distribution = 'u';
                        minD = Integer.parseInt(args[++i]);
                        maxD = Integer.parseInt(args[++i]);
                        if (minD > maxD) {
                            throw new Exception("minimum cannot be greater than maximum");
                        }
                        break;
                    case "-g":
                        distribution = 'g';
                        mean = Double.parseDouble(args[++i]);
                        stdev = Double.parseDouble(args[++i]);
                        break;
                    case "-e":
                        distribution = 'x';
                        lambda = Double.parseDouble(args[++i]);
                    default:
                        break;
                }
            }
            if (portA == 0 || portB == 0 || channelPort == 0 || portA == portB || portB == channelPort || portA == channelPort){
                throw new Exception("Illegal ports");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println("Probably Illegal arguments, read the documentation");
            return;
        }
        try {
            new UnreliableChannel(distribution, minD, maxD, mean, stdev, lambda, channelPort, portA, portB, goodP, burstP, goodToBurst, burstToGood).channelStuff();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
//class for simulating burst packet loss instead of just normal loss, using gilbert model, i did ask claude what the best way to implement bursts would be
class BurstLossSimulator {
    //the good and bad probabilities
    double goodP;
    double badP;

    //state transition probabilities
    double burstToGood;
    double goodToBurst;

    //state indicator, if 0 then it's good and if 1 then burst
    int state = 0;
    
    public BurstLossSimulator(double goodP, double badP, double goodToBurst, double burstToGood) {
        this.goodP = goodP;
        this.badP = badP;
        this.goodToBurst = goodToBurst;
        this.burstToGood = burstToGood;
    }

    //synchronized to avoid thread problems since all threads will be calling this
    public synchronized boolean drop() {
        //get the probs for both the drop and state transition
        double lossProb = ThreadLocalRandom.current().nextDouble();
        double transitionProb = ThreadLocalRandom.current().nextDouble();

        boolean drop = lossProb < (state == 0 ? goodP : badP);

        state = (state == 0 ? (transitionProb < goodToBurst ? 1 : 0) : (transitionProb < burstToGood ? 0 : 1));

        return drop;
    }
}