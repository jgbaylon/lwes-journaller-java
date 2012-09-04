package org.lwes.journaller;

/**
 * User: fmaritato
 * Date: Apr 22, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;
import org.lwes.EventSystemException;

import java.io.IOException;
import java.net.Socket;


import static org.junit.Assert.*;

public class UnicastJournallerTest extends BaseJournallerTest {
    private transient Log log = LogFactory.getLog(UnicastJournallerTest.class);

    @Test
    public void testUnicastJournaller()
            throws IOException, CmdLineException, EventSystemException {
        String[] args = {"-a","192.168.56.1","-p", "9191" };
        Journaller uj = new Journaller();
        uj.parseArguments(args);
        uj.initialize();
        String socketClass= uj.getEventHandler().getSocket().getClass().toString();
        assertEquals("Port value is wrong", 9191, uj.getPort());
        assertEquals("Address value is wrong", "192.168.56.1", uj.getAddress());
        assertEquals("Socket is not DatagramSocket","class java.net.DatagramSocket", socketClass);
        uj.shutdown();

    }

    @Test
    public void testMulticastJournaller()
            throws IOException, CmdLineException, EventSystemException {
        String[] args = {"-a","224.1.1.11","-p", "9191" };
        Journaller mj = new Journaller();
        mj.parseArguments(args);
        mj.initialize();

        String socketClass= mj.getEventHandler().getSocket().getClass().toString();
        assertEquals("Port value is wrong", 9191, mj.getPort());
        assertEquals("Address value is wrong", "224.1.1.11", mj.getAddress());
        assertEquals("Socket is not MulticastSocket","class java.net.MulticastSocket", socketClass);
        mj.shutdown();

    }

}