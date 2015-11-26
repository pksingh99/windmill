package com.apple.akane.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.apple.akane.core.CPU;
import com.apple.akane.core.tasks.VoidTask;
import com.apple.akane.utils.IOUtils;

public class ServerSocket implements AutoCloseable
{
    private final CPU cpu;
    private final ServerSocketChannel channel;
    private final Selector selector;
    private final VoidTask<Channel> onAccept;

    public ServerSocket(CPU cpu, Selector selector, InetSocketAddress address, VoidTask<Channel> onAccept) throws IOException
    {
        this.cpu = cpu;
        this.selector = selector;
        this.channel = ServerSocketChannel.open();
        this.onAccept = onAccept;

        java.net.ServerSocket socket = channel.socket();

        channel.configureBlocking(false);
        socket.setReuseAddress(true);
        socket.setSoTimeout(0);
        socket.bind(address);

        channel.register(selector, SelectionKey.OP_ACCEPT, this);
    }

    protected void onAccept() throws IOException
    {
        SocketChannel socket = channel.accept();
        if (socket == null)
            return;

        Channel channel = new Channel(cpu, selector, socket);

        cpu.schedule(() -> {
            onAccept.compute(channel);
            return null;
        });
    }

    @Override
    public void close()
    {
        IOUtils.closeQuietly(channel);
    }
}
