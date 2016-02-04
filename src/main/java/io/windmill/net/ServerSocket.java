package io.windmill.net;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.core.tasks.VoidTask1;
import io.windmill.utils.IOUtils;

public class ServerSocket extends Future<Channel> implements AutoCloseable
{
    private final ServerSocketChannel channel;
    private final VoidTask1<Channel> onAccept;
    private final VoidTask1<Throwable> onFailure;

    public ServerSocket(CPU cpu,
                        Selector selector,
                        InetSocketAddress address,
                        VoidTask1<Channel> onAccept,
                        VoidTask1<Throwable> onFailure)
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
            cpu.schedule(() -> onFailure.compute(e));
            server = null;
        }

        this.channel = server;
        this.onAccept = onAccept;
        this.onFailure = onFailure;
    }

    protected void onAccept()
    {
        cpu.getSocket().register(channel, onAccept, onFailure);
    }

    @Override
    public void close()
    {
        IOUtils.closeQuietly(channel);
    }
}
