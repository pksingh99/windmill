package com.apple.akane.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;

import com.apple.akane.core.CPU;
import com.apple.akane.core.tasks.VoidTask;
import com.apple.akane.utils.IOUtils;

public class Network implements AutoCloseable
{
    protected final CPU cpu;
    protected final Selector selector;

    public Network(CPU cpu) throws IOException
    {
        this.cpu = cpu;
        this.selector = SelectorProvider.provider().openSelector();
    }

    public ServerSocket listen(InetSocketAddress address, VoidTask<Channel> onAccept) throws IOException
    {
        return new ServerSocket(cpu, selector, address, onAccept);
    }

    public void poll() throws IOException
    {
        int ready = selector.selectNow();
        if (ready == 0)
            return;

        Iterator<SelectionKey> readyKeys = selector.selectedKeys().iterator();
        while (readyKeys.hasNext())
        {
            SelectionKey key = readyKeys.next();
            readyKeys.remove();

            if (!key.isValid())
            {
                IOUtils.closeQuietly(((AutoCloseable) key.attachment()));
                key.cancel();
                continue;
            }

            if (key.isAcceptable())
            {
                ((ServerSocket) key.attachment()).onAccept();
            }
            else if (key.isReadable())
            {
                ((Channel) key.attachment()).onRead();
            }
            else if (key.isWritable())
            {
                ((Channel) key.attachment()).onWrite();
            }
        }
    }

    @Override
    public void close()
    {
        IOUtils.closeQuietly(selector);
    }
}
