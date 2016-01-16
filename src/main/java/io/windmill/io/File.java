package io.windmill.io;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import io.windmill.core.Future;
import io.windmill.net.Channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class File
{
    private final IOService io;
    private final FileChannel backend;

    File(IOService ioService, RandomAccessFile file)
    {
        this.io = ioService;
        this.backend = file.getChannel();
    }

    public Future<Integer> write(byte[] buffer)
    {
        return write(Unpooled.wrappedBuffer(buffer));
    }

    public Future<Integer> write(ByteBuf buffer)
    {
        return io.schedule(() -> buffer.readBytes(backend, buffer.readableBytes()));
    }

    public Future<ByteBuf> read(int size)
    {
        return io.schedule(() -> {
            ByteBuf buffer = Unpooled.buffer(size);

            do
            {
                buffer.writeBytes(backend, buffer.writableBytes());
            }
            while (buffer.writableBytes() != 0);

            return buffer;
        });
    }

    public Future<Void> transferTo(Channel channel, long offset, long length)
    {
        return channel.getOutput().transferFrom(backend, offset, length);
    }

    public Future<Void> seek(long position)
    {
        return io.schedule(() -> {
            backend.position(position);
            return null;
        });
    }

    public Future<Void> sync()
    {
        return io.schedule(() -> {
            backend.force(true);
            return null;
        });
    }

    public Future<Void> close()
    {
        return io.schedule(() -> {
            backend.close();
            return null;
        });
    }
}
