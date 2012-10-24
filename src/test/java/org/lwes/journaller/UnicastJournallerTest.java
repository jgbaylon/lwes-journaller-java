package org.lwes.journaller;

/**
 * @author abohr
 * Date: 09/04/2012
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.lwes.EventSystemException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;


import static org.junit.Assert.*;

public class UnicastJournallerTest extends BaseJournallerTest {
    private transient Log log = LogFactory.getLog(UnicastJournallerTest.class);

    @Test
    public void testUnicastJournaller()
            throws IOException, CmdLineException, EventSystemException {
        String testAddress = "127.0.0.1";
        InetAddress ia = InetAddress.getByName( testAddress );
        ServerSocket serverSocket = new ServerSocket(0, 1, ia );
        String freePort = Integer.toString( serverSocket.getLocalPort() );
        serverSocket.close();
        String[] args = {"-a",testAddress,"-p", freePort };
        Journaller uj = new Journaller();
        CmdLineParser parser = null;
        try {
            parser = new CmdLineParser(uj);
            parser.parseArgument(args);
        }
        catch (CmdLineException e) {
        	fail(e.getMessage());
        }
        uj.initialize();
        String socketClass= uj.getEventHandler().getSocket().getClass().toString();
        assertEquals("Port value is wrong", Integer.parseInt(freePort), uj.getPort());
        assertEquals("Address value is wrong", testAddress, uj.getAddress());
        assertEquals("Socket is not DatagramSocket","class java.net.DatagramSocket", socketClass);
        uj.shutdown();

    }

    @Test
    public void testMulticastJournaller()
            throws IOException, CmdLineException, EventSystemException {
        String[] args = {"-a","224.1.1.11","-p", "9191" };
        Journaller mj = new Journaller();
        CmdLineParser parser = null;
        try {
            parser = new CmdLineParser(mj);
            parser.parseArgument(args);
        }
        catch (CmdLineException e) {
        	fail(e.getMessage());
        }
        mj.initialize();

        String socketClass= mj.getEventHandler().getSocket().getClass().toString();
        assertEquals("Port value is wrong", 9191, mj.getPort());
        assertEquals("Address value is wrong", "224.1.1.11", mj.getAddress());
        assertEquals("Socket is not MulticastSocket","class java.net.MulticastSocket", socketClass);
        mj.shutdown();

    }

}