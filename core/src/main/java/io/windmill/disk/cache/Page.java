package io.windmill.disk.cache;

import java.io.IOException;
import java.nio.channels.FileChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class Page
{
    public static final   int PAGE_BITS  = 12;
    public static final short PAGE_SIZE  = 1 << PAGE_BITS; // 4K page
    public static final   int BLOCK_BITS = 9; // 1 << 9 = 512
    public static final short BLOCK_SIZE = 1 << 9; // the size of the single transfer block

    private final PageCache tree;
    private final int pageOffset;
    private final ByteBuf buffer;

    // 4K page consists of eight (8) 512 byte blocks,
    // which are the unit of block device transfer,
    // so instead of trying to write whole page back
    // to media, we to try to write only dirty slices
    private byte dirtyBlocks;

    public Page(PageCache tree, int pageOffset, ByteBuf buffer)
    {
        this.tree = tree;
        this.pageOffset = pageOffset;
        this.buffer = buffer;
    }

    public int getOffset()
    {
        return pageOffset;
    }

    public boolean isDirty()
    {
        return dirtyBlocks != 0;
    }

    public int write(short position, ByteBuf data)
    {
        int toWrite = Math.min(data.readableBytes(), PAGE_SIZE - position);

        try
        {
            buffer.setBytes(position, data, toWrite);
            buffer.writerIndex(Math.max(buffer.writerIndex(), position + toWrite));
            markDirty(position, toWrite); // mark all affected blocks as dirty
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

    public void writeTo(FileChannel file, boolean shouldSync) throws IOException
    {
        try
        {
            long offset = pageOffset << PAGE_BITS;
            for (int block = 0; block < Byte.SIZE; block++)
            {
                if ((dirtyBlocks & (1L << block)) == 0)
                    continue; // clean block

                int position = block * BLOCK_SIZE;
                int length = Math.min(buffer.readableBytes() - position, BLOCK_SIZE);

                buffer.getBytes(position, file.position(offset + position), length);
            }

            if (shouldSync)
                file.force(true);
        }
        finally
        {
            tree.markPageClean(pageOffset);
            dirtyBlocks = 0;
        }
    }

    private void markDirty(short position, int size)
    {
        int block = position >> BLOCK_BITS;
        int numBlocks = ((blockOffset(position) + size) >> BLOCK_BITS) + 1;

        for (int i = 0; i < numBlocks; i++)
            setBlockDirty((byte) (block + i));
    }

    private void setBlockDirty(byte blockIndex)
    {
        dirtyBlocks |= (1L << blockIndex);
    }

    private short blockOffset(short position)
    {
        return (short) (position - alignToBlock(position));
    }

    private short alignToBlock(short position)
    {
        return  (short) (position & ~(BLOCK_SIZE - 1));
    }
}
