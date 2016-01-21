package io.windmill.net;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.utils.IOUtils;

public class ClientSocket extends Future<Channel> implements AutoCloseable
{
    private final Selector selector;
    private final SocketChannel channel;

    public ClientSocket(CPU cpu, Selector selector, InetSocketAddress address)
    {
        super(cpu);

        SocketChannel client;

        try
        {
            client = SocketChannel.open();

            client.configureBlocking(false);
            client.connect(address);

            client.register(selector, SelectionKey.OP_CONNECT, this);
        }
        catch (Exception | Error e)
        {
            setFailure(e);
            client = null;
        }

        this.selector = selector;
        this.channel = client;
    }

    public void onConnect()
    {
        try
        {
            if (channel.isConnectionPending())
            {
                channel.finishConnect();
                setValue(new Channel(cpu, selector, channel));
            }
        }
        catch (Exception | Error e)
        {
            setFailure(e);
        }
    }

    @Override
    public void close() throws Exception
    {
        IOUtils.closeQuietly(channel);
    }
}
