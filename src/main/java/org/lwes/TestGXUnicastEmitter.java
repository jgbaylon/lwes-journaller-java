package org.lwes;
/**
 * User: fmaritato
 * Date: Apr 20, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.lwes.emitter.UnicastEventEmitter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.Random;

public class TestGXUnicastEmitter extends UnicastEventEmitter implements Runnable {

    private static transient Log log = LogFactory.getLog(TestGXUnicastEmitter.class);

    @Option(name="-a", aliases = "--address")
    private String uniAddress;

    @Option(name="-p", aliases="--uniPort")
    private int uniPort;

    @Option(name="-i", aliases = "--interface")
    private String multicastInterface;

    @Option(name="-t", aliases = "--ttl")
    private int ttl = 1;

    @Option(name = "-n", aliases = "--number")
    private int number = 1;

    @Option(name = "-s", aliases = "--seconds")
    private int seconds = 1;

    @Option(name = "-b", aliases = "--break")
    private int breakSeconds = 0;

    @Option(name = "-r", aliases = "--rotate")
    private boolean sendRotate = false;

	@Option(name = "--files", usage = "The files to be used for sample value")
	private String files;

    @Override
    public void initialize() throws IOException {
        setAddress(InetAddress.getByName(uniAddress));
        setPort(uniPort);
        super.initialize();
    }

    public void run() {

        try {
            initialize();

            // if we are supposed to send a rotate message, just do that and exit.
            if (isSendRotate()) {
                Event evt = createEvent("Command::Rotate", false);
                emit(evt);
            }
            else {
                for (int i = 0; i < getSeconds(); i++) {
					LinkedList<Event> events = new LinkedList<Event>();
					GxEventGenerator generator = new GxEventGenerator( files );
					events = generator.createTransaction( getNumber() );
					System.out.println("Got back "+events.size() +" events");
                    for( Event evt : events){
						emit(evt);
					}

                    Thread.sleep(getBreakSeconds() * 1000);
                }
            }
        }
        catch (Exception e) {
			e.printStackTrace();
            log.error(e.getMessage(), e);
        }

    }

    private static int showRandomInteger(int aStart, int aEnd, Random aRandom){
        if ( aStart > aEnd ) {
            throw new IllegalArgumentException("Start cannot exceed End.");
        }
        //get the range, casting to long to avoid overflow problems
        long range = (long)aEnd - (long)aStart + 1;
        // compute a fraction of the range, 0 <= frac < range
        long fraction = (long)(range * aRandom.nextDouble());
        int randomNumber =  (int)(fraction + aStart);
        return randomNumber;
    }


    protected void parseArguments(String[] args) throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);
    }

    public static void main(String[] args) {
        TestGXUnicastEmitter te = new TestGXUnicastEmitter();
        try {
            te.parseArguments(args);
        }
        catch (CmdLineException e) {
            log.error(e.getMessage(), e);
        }
        te.run();
    }

    public boolean isSendRotate() {
        return sendRotate;
    }

    public void setSendRotate(boolean sendRotate) {
        this.sendRotate = sendRotate;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public int getSeconds() {
        return seconds;
    }

    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }

    public int getBreakSeconds() {
        return breakSeconds;
    }

    public void setBreakSeconds(int breakSeconds) {
        this.breakSeconds = breakSeconds;
    }

    public String getUniAddr() {
        return uniAddress;
    }

    public void setUniAddr(String uniAddress) {
        this.uniAddress = uniAddress;
    }

    public int getUniPort() {
        return uniPort;
    }

    public void setUniPort(int port) {
        this.uniPort = port;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
