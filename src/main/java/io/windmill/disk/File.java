package io.windmill.disk;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.disk.cache.Page;
import io.windmill.disk.cache.FileCache;
import io.windmill.net.Channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.windmill.utils.Futures;

public class File
{
    private final CPU cpu;
    private final FileCache cache;

    File(CPU cpu, RandomAccessFile file)
    {
        this(cpu, new FileCache(cpu, file.getChannel()));
    }

    private File(CPU cpu, FileCache cache)
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
    public Future<Long> write(long position, byte[] buffer)
    {
        return write(position, Unpooled.wrappedBuffer(buffer));
    }

    public Future<Long> write(long position, ByteBuf buffer)
    {
        return seek(position).flatMap((context) -> context.write(buffer));
    }

    public Future<ByteBuf> read(long position, int size)
    {
        return seek(position).flatMap((context) -> context.read(size));
    }

    public Future<Integer> transferTo(Channel channel, long position, long length)
    {
        return cache.transferTo(channel, position, length);
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
        return Futures.constantFuture(cpu, new FileContext(this, newPosition));
    }

    public Future<Integer> sync()
    {
        return cache.sync();
    }

    public Future<Void> close()
    {
        return cache.close();
    }

    Future<List<Page>> requestPages(long position, int size)
    {
        int pageCount  = size / Page.PAGE_SIZE + 1;
        int pageOffset = alignToPage(position);

        // optimization for single page reads
        if (pageCount == 1)
            return cache.getOrCreate(pageOffset).map(Collections::singletonList);

        List<Future<Page>> pages = new ArrayList<>(pageCount);

        for (int i = 0; i < pageCount; i++)
            pages.add(cache.getOrCreate(pageOffset + i));

        return cpu.sequence(pages);
    }

    protected static int alignToPage(long position)
    {
        return (int) (position & ~(Page.PAGE_SIZE - 1));
    }
}
