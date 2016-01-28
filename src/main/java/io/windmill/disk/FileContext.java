package io.windmill.disk;

import io.windmill.core.Future;
import io.windmill.disk.cache.Page;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class FileContext
{
    private final File file;
    private long position;

    public FileContext(File file, long position)
    {
        this.file = file;
        this.position = position;
    }

    public Future<Long> write(byte[] buffer)
    {
        return write(Unpooled.wrappedBuffer(buffer));
    }

    public Future<Long> write(ByteBuf buffer)
    {
        long startPosition = position;
        return file.requestPages(position, buffer.readableBytes()).map((pages) -> {
            short pagePosition = getPagePosition(position);

            for (Page page : pages)
            {
                position += page.write(pagePosition, buffer);
                pagePosition = 0;
            }

            return startPosition;
        });
    }

    public Future<ByteBuf> read(int size)
    {
        return file.requestPages(position, size).map((pages) -> {
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

    private static short getPagePosition(long position)
    {
        return (short) (position - File.alignToPage(position));
    }
}
