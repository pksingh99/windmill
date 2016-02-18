package io.windmill.net;

import java.io.IOError;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.core.tasks.VoidTask1;
import io.windmill.utils.IOUtils;

public class Network implements AutoCloseable
{
    protected final CPU cpu;
    protected final Selector selector;

    public Network(CPU cpu)
    {
        this.cpu = cpu;
        this.selector = openSelector();
    }

    public void listen(InetSocketAddress address, VoidTask1<Channel> onAccept, VoidTask1<Throwable> onFailure)
    {
        new ServerSocket(cpu, selector, address, onAccept, onFailure);
    }

    public Future<Channel> connect(InetSocketAddress address)
    {
        return new ClientSocket(cpu, selector, address);
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
            else if (key.isConnectable())
            {
                ((ClientSocket) key.attachment()).onConnect();
            }
        }
    }

    @Override
    public void close()
    {
        IOUtils.closeQuietly(selector);
    }

    public Selector getSelector()
    {
        return selector;
    }

    private static Selector openSelector()
    {
        try
        {
            return SelectorProvider.provider().openSelector();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }
}
