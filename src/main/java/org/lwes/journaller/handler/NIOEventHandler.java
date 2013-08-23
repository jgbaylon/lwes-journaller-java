package org.lwes.journaller.handler;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.journaller.DeJournaller;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.listener.DatagramQueueElement;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class NIOEventHandler extends AbstractFileEventHandler {

    private static transient Log log = LogFactory.getLog(NIOEventHandler.class);

    private FileChannel channel = null;
    private FileChannel tmpChannel = null;
    private FileChannel oldChannel = null;
    
    private ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<ByteBuffer>(){
    	protected ByteBuffer initialValue() {
    		return ByteBuffer.allocate(DeJournaller.MAX_HEADER_SIZE + DeJournaller.MAX_BODY_SIZE);
    	};
    };

    public NIOEventHandler() {
    }

    public NIOEventHandler(String filePattern) throws IOException {
        setFilenamePattern(filePattern);
        String fn = generateFilename();
        createOutputStream(fn);
        swapOutputStream();
        setFilename(fn);
    }

    public void createOutputStream(String filename) throws IOException {
        tmpChannel = FileChannel.open(
        		new File(filename).toPath(), 
        		StandardOpenOption.WRITE, 
        		StandardOpenOption.APPEND, 
        		StandardOpenOption.DSYNC);
    }

    public void handleEvent(DatagramQueueElement element) throws IOException {
        DatagramPacket packet = element.getPacket();

        ByteBuffer buffer = localBuffer.get();

        EventHandlerUtil.writeHeader(packet.getLength(),
                                     element.getTimestamp(),
                                     packet.getAddress(),
                                     packet.getPort(),
                                     getSiteId(),
                                     buffer);

        buffer.put(packet.getData());
        buffer.flip();
        
        synchronized (lock) {
            channel.write(buffer);
        }
        buffer.clear();
    }

    public String getFileExtension() {
        return ".log";
    }

    public void swapOutputStream() {
        oldChannel = channel;
        channel = tmpChannel;
        tmpChannel = null;
    }

    public void closeOutputStream() throws IOException {
        if (oldChannel != null) {
            oldChannel.close();
            oldChannel = null;
        }
    }

    public ObjectName getObjectName() throws MalformedObjectNameException {
        return new ObjectName("org.lwes:name=NIOEventHandler");
    }
}
