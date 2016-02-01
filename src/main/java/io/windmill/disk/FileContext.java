package io.windmill.disk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.windmill.core.Future;
import io.windmill.disk.cache.Page;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.windmill.net.Channel;

public class FileContext
{

    private final File file;
    private long position;

    public FileContext(File file, long position)
    {
        this.file = file;
        this.position = position;
    }

    public long getPosition()
    {
        return position;
    }

    public Future<FileContext> write(byte[] buffer)
    {
        return write(Unpooled.wrappedBuffer(buffer));
    }

    public Future<FileContext> write(ByteBuf buffer)
    {
        return requestPages(position, buffer.readableBytes()).map((pages) -> {
            short pagePosition = getPagePosition(position);

            for (Page page : pages)
            {
                position += page.write(pagePosition, buffer);
                pagePosition = 0;
            }

            return this;
        });
    }

    public Future<ByteBuf> read(int size)
    {
        return requestPages(position, size).map((pages) -> {
            ByteBuf buffer = Unpooled.buffer(size);

            int readSize = size;
            short offset = getPagePosition(position);
            for (Page page : pages)
            {
                int toRead = Math.min(Page.PAGE_SIZE - offset, readSize);
                buffer.writeBytes(page.read(offset, toRead));

                offset = 0; // only first page has >= 0 offset
                readSize -= toRead;
                position += toRead;
            }

            return buffer;
        });
    }

    public Future<Long> transferTo(Channel channel, long size)
    {
        int pageOffset = alignToPage(position) >> Page.PAGE_BITS;
        short offset = getPagePosition(position);

        List<Future<Long>> transfers = new ArrayList<>();
        while (size > 0)
        {
            int toTransfer = (int) Math.min(Page.PAGE_SIZE - offset, size);
            transfers.add(file.cache.transferPage(channel, pageOffset, offset, toTransfer));

            pageOffset++;
            offset = 0; // only first page has aligned offset
            size -= toTransfer;
        }

        return file.cpu.sequence(transfers).map((sizes) -> {
            long total = 0;
            for (Long transferSize : sizes)
                total += transferSize;

            return total;
        });
    }

    private Future<List<Page>> requestPages(long position, int size)
    {
        int pageCount  = size / Page.PAGE_SIZE + 1;
        int pageOffset = alignToPage(position) >> Page.PAGE_BITS;

        // optimization for single page reads
        if (pageCount == 1)
        {
            file.markPageAccess(pageOffset);
            return file.cache.getOrCreate(pageOffset).map(Collections::singletonList);
        }

        List<Future<Page>> pages = new ArrayList<>(pageCount);
        for (int i = 0; i < pageCount; i++)
        {
            file.markPageAccess(pageOffset);
            pages.add(file.cache.getOrCreate(pageOffset + i));
        }

        return file.cpu.sequence(pages);
    }

    private static short getPagePosition(long position)
    {
        return (short) (position - alignToPage(position));
    }

    private static int alignToPage(long position)
    {
        return (int) (position & ~(Page.PAGE_SIZE - 1));
    }
}
