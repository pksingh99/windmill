package io.windmill.net;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.utils.IOUtils;

public class ServerSocket extends Future<Channel> implements AutoCloseable
{
    private final ServerSocketChannel channel;

    public ServerSocket(CPU cpu, Selector selector, InetSocketAddress address)
    {
        super(cpu);

        ServerSocketChannel server;

        try
        {
            server = ServerSocketChannel.open();

            java.net.ServerSocket socket = server.socket();

            server.configureBlocking(false);

            socket.setReuseAddress(true);
            socket.setSoTimeout(0);
            socket.bind(address);

            server.register(selector, SelectionKey.OP_ACCEPT, this);
        }
        catch (Exception | Error e)
        {
            setFailure(e);
            server = null;
        }

        this.channel = server;
    }

    protected void onAccept()
    {
        cpu.getSocket().register(channel, this);
    }

    @Override
    public void close()
    {
        IOUtils.closeQuietly(channel);
    }
}
