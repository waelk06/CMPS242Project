import java.util.*;
import java.net.*;

public class UnreliableChannel {
    //different distributions for the delay
    private boolean uniformDelay;
    private boolean gaussianDelay;
    private boolean exponentialDelay;

    //min and max for the uniform distribution
    private int minD;
    private int maxD;

    //mean and stdev for the gaussian distribution
    private double mean;
    private double stdev;

    //lambda for the exponential distribution
    private double lambda;

    private Random random = new Random();

    //function for calculating the delay
    public long calcDelay() {
        long delay = 0;

        if (uniformDelay) {
            delay = random.nextInt((maxD - minD) + 1) + minD;
        } else if (gaussianDelay) {
            delay = (long) (stdev*random.nextGaussian() + mean);
        } else if (exponentialDelay) {
            delay = (long) (-Math.log(random.nextDouble())/lambda);
        } 

        return delay;
    }
}