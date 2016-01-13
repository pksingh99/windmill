package io.windmill.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import io.windmill.core.CPU;
import io.windmill.core.tasks.VoidTask1;
import io.windmill.utils.IOUtils;

public class ServerSocket implements AutoCloseable
{
    private final CPU cpu;
    private final ServerSocketChannel channel;
    private final VoidTask1<Channel> onAccept;
    private final VoidTask1<Throwable> onFailure;

    public ServerSocket(CPU cpu, Selector selector, InetSocketAddress address, VoidTask1<Channel> onAccept, VoidTask1<Throwable> onFailure) throws IOException
    {
        this.cpu = cpu;
        this.channel = ServerSocketChannel.open();
        this.onAccept = onAccept;
        this.onFailure = onFailure;

        java.net.ServerSocket socket = channel.socket();

        channel.configureBlocking(false);
        socket.setReuseAddress(true);
        socket.setSoTimeout(0);
        socket.bind(address);

        channel.register(selector, SelectionKey.OP_ACCEPT, this);
    }

    protected void onAccept() throws IOException
    {
        cpu.getSocket().register(channel.accept(), onAccept, onFailure);
    }

    @Override
    public void close()
    {
        IOUtils.closeQuietly(channel);
    }
}
