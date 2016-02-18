package io.windmill.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.core.Status;
import io.windmill.core.Status.Flag;
import io.windmill.core.tasks.Task1;
import io.windmill.core.tasks.VoidTask1;
import io.windmill.net.io.InputStream;
import io.windmill.net.io.OutputStream;
import io.windmill.utils.IOUtils;

/**
 * Windmill's equivalent of {@link java.nio.channels.SocketChannel},
 * providing an interface to stream-oriented connecting sockets.
 * Each {@link Channel} is assigned to a specific {@link CPU} and all work
 * on the socket or its streams. Channel's can be obtained using
 * {@link CPU#connect(InetSocketAddress)} or
 * {@link CPU#listen(InetSocketAddress, VoidTask1, VoidTask1)}
 */
public class Channel implements AutoCloseable
{
    private final CPU cpu;
    private final SocketChannel channel;

    private final InputStream input;
    private final OutputStream output;

    public Channel(CPU cpu, Selector selector, SocketChannel channel) throws IOException
    {
        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        SelectionKey key = channel.register(selector, SelectionKey.OP_READ, this);

        this.cpu = cpu;
        this.channel = channel;
        this.input = new InputStream(cpu, channel);
        this.output = new OutputStream(cpu, key, channel);
    }

    /**
     * Perform a task continuously on the {@link CPU} this {@link Channel} is assigned to.
     * Similar to {@link CPU#repeat(Task1)}
     * @param task the work to perform
     * @param <O> the type of value the work produces
     */
    public <O> void loop(Task1<CPU, Future<O>> task)
    {
        cpu.repeat((cpu) -> task.compute(cpu).map((v) -> Status.of(Flag.CONTINUE)));
    }

    /**
     * @return the {@link InputStream} that can be used to read data sent to this {@link Channel}
     */
    public InputStream getInput()
    {
        return input;
    }

    /**
     * @return the {@link InputStream} that can be used to write data to this {@link Channel}
     */
    public OutputStream getOutput()
    {
        return output;
    }

    protected void onRead() throws IOException
    {
        input.triggerRx();
    }

    protected void onWrite()
    {
        output.triggerTx();
    }

    @Override
    public void close()
    {
        input.close();
        output.close();
        IOUtils.closeQuietly(channel);
    }

    @Override
    public String toString()
    {
        return String.format("Channel: %s", channel);
    }
}
