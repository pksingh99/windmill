package io.windmill.disk;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.disk.cache.PageCache;
import io.windmill.net.Channel;
import io.windmill.utils.Futures;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class File
{
    protected final CPU cpu;
    protected final PageCache cache;
    protected final IOService ioService;

    File(CPU cpu, IOService ioService, RandomAccessFile file)
    {
        this(cpu, ioService, file.getChannel());
    }

    protected File(CPU cpu, IOService ioService, FileChannel file)
    {
        this(cpu, new PageCache(cpu, file), ioService);
    }

    private File(CPU cpu, PageCache cache, IOService ioService)
    {
        this.cpu = cpu;
        this.cache = cache;
        this.ioService = ioService;
    }

    /**
     * Write given buffer to specified position and return a file context
     * for further chaining of the read/write requests.
     *
     * @param position The position to write data to.
     * @param buffer The buffer to write to file.
     *
     * @return The file context with resulting file position for request chaining.
     */
    public Future<FileContext> write(long position, byte[] buffer)
    {
        return write(position, Unpooled.wrappedBuffer(buffer));
    }

    /**
     * Write given buffer to specified position and return a file context
     * for further chaining of the read/write requests.
     *
     * @param position The position to write new data to.
     * @param buffer The buffer of bytes to write to given position.
     *
     * @return The file context with resulting file position for request chaining.
     */
    public Future<FileContext> write(long position, ByteBuf buffer)
    {
        return seek(position).flatMap((context) -> context.write(buffer));
    }

    /**
     * Read n bytes from specified position in the file.
     *
     * @param position The file position to start reading from.
     * @param size The amount of bytes to read from the give position.
     *
     * @return The buffer containing requested bytes, might be empty if read
     *         was attempted outside of current file bounds.
     */
    public Future<ByteBuf> read(long position, int size)
    {
        return seek(position).flatMap((context) -> context.read(size));
    }

    /**
     * Transfer n bytes starting at specified position to the network.
     *
     * @param channel The channel to transfer bytes to.
     * @param position The file position to start transfer from.
     * @param length The amount of bytes to transfer.
     *
     * @return The number of bytes transferred, might be less then requested length
     *         if the file length bound was crossed.
     */
    public Future<Long> transferTo(Channel channel, long position, long length)
    {
        return seek(position).flatMap((context) -> context.transferTo(channel, length));
    }

    /**
     * Mark page identified by given offset (page-aligned) as accessed.
     * @param pageOffset The offset of the page aligned on page boundary (Page.PAGE_SIZE)
     */
    protected final void markPageAccess(int pageOffset)
    {
        ioService.markPageAccessed(this, pageOffset);
    }

    /**
     * Evict page identified by given offset from the page cache (offset must be page aligned).
     * @param pageOffset The offset of the page to evict.
     */
    protected final void evictPage(int pageOffset)
    {
        cache.evictPage(pageOffset);
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

    /**
     * Write all of the dirty pages back to block device.
     * NOTE: This method forces fsync after all pages are written.
     *
     * @return The number of pages written.
     */
    public Future<Integer> sync()
    {
        return cache.sync();
    }

    public Future<Void> close()
    {
        return cache.close((page) -> ioService.markPageEvicted(this, page.getOffset()));
    }
}
