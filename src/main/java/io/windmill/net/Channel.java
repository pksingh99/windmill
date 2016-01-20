package io.windmill.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.core.tasks.Task2;
import io.windmill.net.io.InputStream;
import io.windmill.net.io.OutputStream;
import io.windmill.utils.IOUtils;

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

    public <O> void loop(Task2<CPU, O, Future<O>> task)
    {
        cpu.repeat(task::compute);
    }

    public InputStream getInput()
    {
        return input;
    }

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
