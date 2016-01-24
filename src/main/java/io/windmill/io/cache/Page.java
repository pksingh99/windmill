package io.windmill.io.cache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class Page
{
    public static final short PAGE_SIZE = 1 << 12; // 4K page

    private final RadixTree tree;
    private final int pageOffset;
    private final ByteBuf buffer;

    Page(RadixTree tree, int pageOffset)
    {
        this.tree = tree;
        this.pageOffset = pageOffset;
        this.buffer = Unpooled.buffer(PAGE_SIZE, PAGE_SIZE);
    }

    public void write(short position, ByteBuf data)
    {
        if (data.readableBytes() >= PAGE_SIZE)
            throw new IllegalArgumentException();

        tree.markPageDirty(pageOffset);
        buffer.writerIndex(position).writeBytes(data, data.readableBytes());
    }
}
