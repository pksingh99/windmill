package io.windmill.disk;

import java.io.RandomAccessFile;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.disk.cache.PageCache;
import io.windmill.net.Channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.windmill.utils.Futures;

public class File
{
    protected final CPU cpu;
    protected final PageCache cache;

    File(CPU cpu, RandomAccessFile file)
    {
        this(cpu, new PageCache(cpu, file.getChannel()));
    }

    private File(CPU cpu, PageCache cache)
    {
        this.cpu = cpu;
        this.cache = cache;
    }


    /**
     * Write buffer to the file and return file offset where it starts.
     *
     * @param position The position to write data to.
     * @param buffer The buffer to write to file.
     *
     * @return The file offset where buffer is going to be located in the file.
     */
    public Future<FileContext> write(long position, byte[] buffer)
    {
        return write(position, Unpooled.wrappedBuffer(buffer));
    }

    public Future<FileContext> write(long position, ByteBuf buffer)
    {
        return seek(position).flatMap((context) -> context.write(buffer));
    }

    public Future<ByteBuf> read(long position, int size)
    {
        return seek(position).flatMap((context) -> context.read(size));
    }

    public Future<Long> transferTo(Channel channel, long position, long length)
    {
        return seek(position).flatMap((context) -> context.transferTo(channel, length));
    }

    /**
     * Make a copy of the current file instance and set position to specified one,
     * allows for chaining of the reads and writes without affecting global context e.g.
     *
     * file.seek(pos).flatMap((context) -> context.read(n).flatMap((bytes) -> ...).
     *
     * @param newPosition The position to set to the file.
     *
     * @return The copy of the current file with position set to specified one.
     */
    public Future<FileContext> seek(long newPosition)
    {
        return Futures.constantFuture(cpu, new FileContext(cpu, cache, newPosition));
    }

    public Future<Integer> sync()
    {
        return cache.sync();
    }

    public Future<Void> close()
    {
        return cache.close();
    }
}
