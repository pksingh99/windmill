package com.apple.akane.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import com.apple.akane.core.CPU;
import com.apple.akane.core.tasks.VoidTask;
import com.apple.akane.utils.IOUtils;

public class ServerSocket implements AutoCloseable
{
    private final CPU cpu;
    private final ServerSocketChannel channel;
    private final VoidTask<Channel> onAccept;
    private final VoidTask<Throwable> onFailure;

    public ServerSocket(CPU cpu, Selector selector, InetSocketAddress address, VoidTask<Channel> onAccept, VoidTask<Throwable> onFailure) throws IOException
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
