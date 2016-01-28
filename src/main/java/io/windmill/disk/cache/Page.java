package io.windmill.disk.cache;

import java.io.IOException;
import java.nio.channels.FileChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class Page
{
    public static final short PAGE_SIZE = 1 << 12; // 4K page

    private final FileCache tree;
    private final int pageOffset;
    private final ByteBuf buffer;

    public Page(FileCache tree, int pageOffset, ByteBuf buffer)
    {
        this.tree = tree;
        this.pageOffset = pageOffset;
        this.buffer = buffer;
    }

    public int write(short position, ByteBuf data)
    {
        int toWrite = Math.min(data.readableBytes(), PAGE_SIZE - position);

        try
        {
            buffer.writerIndex(position).writeBytes(data, toWrite);
            return toWrite;
        }
        finally
        {
            tree.markPageDirty(pageOffset);
        }
    }

    public ByteBuf read(short position, int size)
    {
        return position > buffer.writerIndex()
                ? Unpooled.EMPTY_BUFFER
                : buffer.copy(position, Math.min(buffer.writerIndex() - position, size));
    }

    public void writeTo(FileChannel file) throws IOException
    {
        buffer.getBytes(0, file.position(pageOffset * PAGE_SIZE), buffer.readableBytes());
        tree.markPageClean(pageOffset);
    }
}
