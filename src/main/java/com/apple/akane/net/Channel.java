package com.apple.akane.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import com.apple.akane.core.CPU;
import com.apple.akane.core.Future;
import com.apple.akane.core.tasks.Task1;
import com.apple.akane.net.io.InputStream;
import com.apple.akane.net.io.OutputStream;
import com.apple.akane.utils.IOUtils;

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

    public <O> void loop(Task1<CPU, Future<O>> task)
    {
        cpu.loop(task);
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
