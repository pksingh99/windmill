package io.windmill.disk.cache;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import io.windmill.core.CPU;
import io.windmill.core.Future;
import io.windmill.net.Channel;
import io.windmill.net.io.OutputStream;
import io.windmill.utils.Futures;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

/**
 * This cache implementation is based on Linux Kernel implementation of Radix Tree (https://lwn.net/Articles/175432/)
 * Having 6 levels of nodes with 64 slots each and 4K pages allows for 17TB files.
 */
public class PageCache
{
    private final static int CACHE_NODE_SHIFT = 6; // 6 bits per node
    private final static int CACHE_NODE_NUM_SLOTS = 1 << CACHE_NODE_SHIFT;
    private final static int CACHE_NODE_SLOT_MASK = CACHE_NODE_NUM_SLOTS - 1;

    private final static long[] HEIGHT_TO_MAX_INDEX = new long[CACHE_NODE_SHIFT];

    static
    {
        for (int i = 0; i < HEIGHT_TO_MAX_INDEX.length; i++)
            HEIGHT_TO_MAX_INDEX[i] = maxIndex(i);
    }

    private Node root = null;

    private final CPU cpu;
    private final FileChannel file;
    private final IntObjectMap<Future<Page>> loadingPages = new IntObjectHashMap<>();

    public PageCache(CPU cpu, FileChannel backingFile)
    {
        this.cpu = cpu;
        this.file = backingFile;
    }

    /**
     * Evict the page at the given offset with write-back if
     * such page turns out to be dirty.
     *
     * @param pageOffset The offset of the page to evict from cache.
     *
     * @return promise to evict a page, which gets set when page is completely evicted.
     */
    public Future<Void> evictPage(int pageOffset)
    {
        Node slot = search(pageOffset);
        if (slot == null || !slot.isDataNode())
            return Futures.voidFuture(cpu);

        Page page = slot.page;

        // remove page reference from the slot
        slot.setPage(null);

        return !page.isDirty()
                ? Futures.voidFuture(cpu)
                : cpu.scheduleIO(() -> { page.writeTo(file, true); return null; });
    }

    /**
     * Iterate over every available page and apply given consumer function.
     *
     * @param pageConsumer The function to apply on existing pages.
     */
    public void forEach(Consumer<Page> pageConsumer)
    {
        if (root == null)
            return;

        if (root.isDataNode())
        {
            pageConsumer.accept(root.page);
            return;
        }

        forEach(root, pageConsumer);
    }

    private void forEach(Node node, Consumer<Page> pageConsumer)
    {
        // it's safe to to use recursion here since it's depth is bounded by CACHE_NODE_SHIFT
        for (int i = 0; i < node.slots.length; i++)
        {
            Node slot = node.slots[i];

            if (slot == null)
                continue;

            if (slot.isDataNode())
                pageConsumer.accept(slot.page);
            else
                forEach(slot, pageConsumer);
        }
    }

    /**
     * Retrieve or allocate new page at given offset
     *
     * @param pageOffset The offset for page in the tree (must be PAGE_SIZE aligned)
     *
     * @return Already existing page which belongs to given offset or newly allocated one.
     */
    public Future<Page> getOrCreate(int pageOffset)
    {
        Node slot = search(pageOffset);
        return slot != null && slot.isDataNode()
                ? Futures.constantFuture(cpu, slot.page)
                : allocatePage(pageOffset);
    }

    /**
     * Search for an appropriate data slot based on the given page offset.
     *
     * @param pageOffset The offset of the page.
     *
     * @return page slot if it's already present in the tree, null otherwise.
     */
    private Node search(int pageOffset)
    {
        assert pageOffset >= 0;

        Node node = root;

        if (node == null || pageOffset > HEIGHT_TO_MAX_INDEX[root.height])
            return null;

        if (node.isDataNode())
        {
            // root can only have data if it's the only page in the tree (hence it's offset is 0)
            return pageOffset > 0 ? null : node;
        }

        int height = root.height;
        int shift  = (height - 1) * CACHE_NODE_SHIFT;

        Node slot;

        do
        {
            int slotIndex = (pageOffset >> shift) & CACHE_NODE_SLOT_MASK;
            slot = node.slots[slotIndex];
            if (slot == null)
                return null;

            node = slot; // move down the tree
            shift -= CACHE_NODE_SHIFT;
            height--;
        }
        while (height > 0);

        return slot;
    }

    /**
     * Mark page at the given offset as "dirty"
     * @param pageOffset The offset of the page to mark
     */
    public void markPageDirty(int pageOffset)
    {
        if (root == null)
            return;

        int height = root.height;
        Node slot  = root;
        int shift  = (height - 1) * CACHE_NODE_SHIFT;

        if (height == 0 || pageOffset > HEIGHT_TO_MAX_INDEX[height])
            return;

        while (height > 0)
        {
            int slotIndex = (pageOffset >> shift) & CACHE_NODE_SLOT_MASK;

            // mark slot at the current height as dirty
            slot.markSlotDirty(slotIndex, true);
            // and move on to the next level
            slot = slot.slots[slotIndex];

            if (slot == null)
                throw new IllegalArgumentException(String.format("slot is empty, height %d, offset %d.", height, slotIndex));

            shift -= CACHE_NODE_SHIFT;
            height--;
        }
    }

    /**
     * Mark page at the given offset as "clean"
     * @param pageOffset The offset of the page to mark
     */
    public void markPageClean(int pageOffset)
    {
        assert pageOffset >= 0;
        if (root == null || pageOffset > HEIGHT_TO_MAX_INDEX[root.height])
            return;

        int offset = 0;
        int height = root.height;
        int shift  = height * CACHE_NODE_SHIFT;
        Node slot  = root;
        Node node  = null;

        while (shift > 0)
        {
            if (slot == null)
                return;

            shift -= CACHE_NODE_SHIFT;
            offset = (pageOffset >> shift) & CACHE_NODE_SLOT_MASK;

            node = slot;
            slot = slot.slots[offset];
        }

        if (slot == null)
            return;

        while (node != null)
        {
            node.markSlotDirty(offset, false);
            if (node.dirtyCount() > 0)
                return;

            pageOffset >>= CACHE_NODE_SHIFT;
            offset = pageOffset & CACHE_NODE_SLOT_MASK;
            node = node.parent;
        }
    }

    public Future<Long> transferPage(Channel channel, int pageOffset, short offset, int size)
    {
        Node slot = search(pageOffset);
        OutputStream out = channel.getOutput();

        return slot == null || !slot.isDataNode()
                ? out.transferFrom(file, (pageOffset << Page.PAGE_BITS) + offset, size)
                : out.writeAndFlush(slot.page.read(offset, size));
    }

    /**
     * @return Sync the cache with file and return number of pages flushed
     */
    public Future<Integer> sync()
    {
        // flush all of the dirty pages in sequence
        return cpu.scheduleIO(() -> {
            int numFlushed = 0;
            for (Page page : getDirtyPages())
            {
                page.writeTo(file, false);
                numFlushed++;
            }

            // after all of the pages are written, let's force fsync
            file.force(true);

            return numFlushed;
        });
    }

    public Future<Void> close()
    {
        return close(null);
    }

    public Future<Void> close(Consumer<Page> pageConsumer)
    {
        Future<Void> closePromise = new Future<>(cpu);

        // let's try to sync up dirty pages first
        // and once it is complete (success or failure)
        // we'll close the file.
        sync().onComplete(() -> {
            if (pageConsumer != null)
                forEach(pageConsumer::accept);

            Future<Void> close = cpu.scheduleIO(() -> {
                file.close();
                return null;
            });

            close.onSuccess(closePromise::setValue);
            close.onFailure(closePromise::setFailure);
        });

        return closePromise;
    }

    /**
     * @return True if tree has at least one dirty page, false otherwise
     */
    public boolean isDirty()
    {
        return root != null && root.dirtyCount() > 0;
    }

    /**
     * @return Number of dirty pages currently in the tree
     */
    public List<Page> getDirtyPages()
    {
        if (!isDirty())
            return Collections.emptyList();

        if (root.height == 0)
            return Collections.singletonList(root.page);

        return getDirtyPages(root, new ArrayList<>());
    }

    private List<Page> getDirtyPages(Node node, List<Page> dirtyPages)
    {
        if (node.dirtyCount() == 0)
            return dirtyPages;

        node.forEachDirty((slot) -> {
            assert slot != null;

            if (slot.isDataNode())
                dirtyPages.add(slot.page);
            else
                getDirtyPages(slot, dirtyPages);
        });

        return dirtyPages;
    }

    private Future<Page> allocatePage(int pageOffset)
    {
        assert pageOffset >= 0;

        if (root == null || pageOffset > HEIGHT_TO_MAX_INDEX[root.height])
            expandTree(pageOffset); // extend a tree to be able to hold given index

        Node slot = root,
             node = null;

        int offset = 0,
            height = root.height,
            shift  = (height - 1) * CACHE_NODE_SHIFT;

        while (height > 0)
        {
            if (slot == null)
            {
                slot = new Node(node, height);

                if (node != null)
                {
                    node.slots[offset] = slot;
                    node.count++;
                }
                else
                    root = slot;
            }

            node = slot;

            offset = (pageOffset >> shift) & CACHE_NODE_SLOT_MASK;
            slot   = (node.slots[offset] != null) ? node.slots[offset] : null;

            shift -= CACHE_NODE_SHIFT;
            height--;
        }

        if (slot != null && slot.isDataNode())
            throw new IllegalStateException(String.format("page slot already exists for position %d", pageOffset));

        Node insertionPoint = new Node(node);

        if (node != null)
        {
            node.count++;
            node.slots[offset] = insertionPoint;
        }
        else
        {
            root = insertionPoint;
        }

        Future<Page> pageFuture = loadingPages.get(pageOffset);

        // requested page is already page-fault'ed and being loaded from disk
        if (pageFuture != null)
            return pageFuture;

        pageFuture = cpu.scheduleIO(() -> {
            ByteBuf buffer = Unpooled.buffer(Page.PAGE_SIZE);
            long position = pageOffset << Page.PAGE_BITS;

            // only try to read if we are in the current file limits
            if (position < file.size())
                buffer.writeBytes(file.position(position), Page.PAGE_SIZE);

            return new Page(this, pageOffset, buffer);
        });

        loadingPages.put(pageOffset, pageFuture);

        pageFuture.onSuccess(insertionPoint::setPage);
        pageFuture.onComplete(() -> loadingPages.remove(pageOffset));

        return pageFuture;
    }

    /**
     * Extend a radix tree so it can store key @index.
     * @param index index key
     */
    private void expandTree(long index)
    {
        assert index >= 0;

        Node node;
        int height = root == null ? 1 : root.height + 1;

        while (index > HEIGHT_TO_MAX_INDEX[height])
            height++;

        if (root == null)
        {
            root = new Node(null, height);
            return;
        }

        do
        {
            int newHeight = root.height + 1;

            node = new Node(null, newHeight);
            node.slots[0] = root;

            if (isDirty()) // if current root has dirty data
                node.markSlotDirty(0, true); // setting 0 element of the new root as dirty

            node.count  = 1;
            root.parent = node;
            root        = node;
            root.height = newHeight;
        }
        while (height > root.height);
    }

    private static class Node
    {
        // height from the bottom
        public int height;

        // number of non-empty slots
        public int count = 0;

        public Node parent;
        public final Node[] slots = new Node[CACHE_NODE_NUM_SLOTS];

        // each bit identifies if slot at that index has dirty data (64-bits for 64 slots)
        public long dirtySlots = 0;

        // assigned only if this Node is "data" node
        private Page page;

        public Node(Node parent)
        {
            this.parent = parent;
        }

        public Node(Node parent, int height)
        {
            this(parent);
            this.height = height;
        }

        public void setPage(Page page)
        {
            this.page = page;
        }

        public boolean isDataNode()
        {
            return page != null;
        }

        public int dirtyCount()
        {
            return Long.bitCount(dirtySlots);
        }

        public void markSlotDirty(int slotIndex, boolean isDirty)
        {
            if (isDirty)
                dirtySlots |= (1L << slotIndex);
            else
                dirtySlots &= ~(1L << slotIndex);
        }

        public void forEachDirty(Consumer<Node> consumer)
        {
            for (int bit = 0; bit < Long.SIZE; bit++)
            {
                if ((dirtySlots & (1L << bit)) != 0)
                    consumer.accept(slots[bit]);
            }
        }
    }

    /**
     * Max possible index for the given height
     *
     * @param height The height
     *
     * @return max index
     */
    private static long maxIndex(int height)
    {
        int width = height * CACHE_NODE_SHIFT;
        int shift = 64 - width; // long = 64 bits

        if (shift < 0)
            return ~0L;

        if (shift >= 64)
            return 0L;

        return ~0L >>> shift;
    }
}
