package io.windmill.io.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

public class RadixTree
{
    private final static int RADIX_TREE_NODE_SHIFT = 6; // 6 bits per node
    private final static int RADIX_TREE_NUM_SLOTS  = 1 << RADIX_TREE_NODE_SHIFT;
    private final static int RADIX_TREE_SLOT_MASK  = RADIX_TREE_NUM_SLOTS - 1;

    private final static long[] HEIGHT_TO_MAX_INDEX = new long[RADIX_TREE_NODE_SHIFT];

    static
    {
        for (int i = 0; i < HEIGHT_TO_MAX_INDEX.length; i++)
            HEIGHT_TO_MAX_INDEX[i] = maxIndex(i);
    }

    private Node root = null;

    /**
     * Retrieve or allocate new page at given offset
     *
     * @param pageOffset The offset for page in the tree (must be PAGE_SIZE aligned)
     *
     * @return Already existing page which belongs to given offset or newly allocated one.
     */
    public Page getOrCreate(int pageOffset)
    {
        Preconditions.checkArgument(pageOffset >= 0);

        Node node = root;

        if (node == null || pageOffset > HEIGHT_TO_MAX_INDEX[root.height])
            return allocatePage(pageOffset);

        if (node.isDataNode())
        {
            // root can only have data if it's the only page in the tree (hence it's offset is 0)
            return (pageOffset > 0) ? allocatePage(pageOffset) : node.page;
        }

        int height = root.height;
        int shift  = (height - 1) * RADIX_TREE_NODE_SHIFT;

        Node slot;

        do
        {
            int slotIndex = (pageOffset >> shift) & RADIX_TREE_SLOT_MASK;
            slot = node.slots[slotIndex];
            if (slot == null)
                return allocatePage(pageOffset);

            node = slot; // move down the tree
            shift -= RADIX_TREE_NODE_SHIFT;
            height--;
        }
        while (height > 0);

        return !slot.isDataNode() ? allocatePage(pageOffset) : slot.page;
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
        int shift  = (height - 1) * RADIX_TREE_NODE_SHIFT;

        if (height == 0 || pageOffset > HEIGHT_TO_MAX_INDEX[height])
            return;

        while (height > 0)
        {
            int slotIndex = (pageOffset >> shift) & RADIX_TREE_SLOT_MASK;

            // mark slot at the current height as dirty
            slot.markSlotDirty(slotIndex, true);
            // and move on to the next level
            slot = slot.slots[slotIndex];

            if (slot == null)
                throw new IllegalArgumentException(String.format("slot is empty, height %d, offset %d.", height, slotIndex));

            shift -= RADIX_TREE_NODE_SHIFT;
            height--;
        }
    }

    /**
     * Mark page at the given offset as "clean"
     * @param pageOffset The offset of the page to mark
     */
    public void markPageClean(int pageOffset)
    {
        Preconditions.checkArgument(pageOffset >= 0);
        if (root == null || pageOffset > HEIGHT_TO_MAX_INDEX[root.height])
            return;

        int offset = 0;
        int height = root.height;
        int shift  = height * RADIX_TREE_NODE_SHIFT;
        Node slot  = root;
        Node node  = null;

        while (shift > 0)
        {
            if (slot == null)
                return;

            shift -= RADIX_TREE_NODE_SHIFT;
            offset = (pageOffset >> shift) & RADIX_TREE_SLOT_MASK;

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

            pageOffset >>= RADIX_TREE_NODE_SHIFT;
            offset = pageOffset & RADIX_TREE_SLOT_MASK;
            node = node.parent;
        }
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
            Preconditions.checkNotNull(slot);

            if (slot.isDataNode())
                dirtyPages.add(slot.page);
            else
                getDirtyPages(slot, dirtyPages);
        });

        return dirtyPages;
    }

    private Page allocatePage(int pageOffset)
    {
        Preconditions.checkArgument(pageOffset >= 0);

        Page page = new Page(this, pageOffset);

        if (root == null || pageOffset > HEIGHT_TO_MAX_INDEX[root.height])
            expandTree(pageOffset); // extend a tree to be able to hold given index

        Node slot = root,
             node = null;

        int offset = 0,
            height = root.height,
            shift  = (height - 1) * RADIX_TREE_NODE_SHIFT;

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

            offset = (pageOffset >> shift) & RADIX_TREE_SLOT_MASK;
            slot   = (node.slots[offset] != null) ? node.slots[offset] : null;

            shift -= RADIX_TREE_NODE_SHIFT;
            height--;
        }

        if (slot != null)
            throw new IllegalStateException(String.format("slot already exists for position %d", pageOffset));

        if (node != null)
        {
            node.count++;
            node.slots[offset] = new Node(node, page);
        }
        else
        {
            root = new Node(null, page);
        }

        return page;
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
        public final Node[] slots = new Node[RADIX_TREE_NUM_SLOTS];

        // each bit identifies if slot at that index has dirty data (64-bits for 64 slots)
        public long dirtySlots = 0;

        // assigned only if this Node is "data" node
        private Page page;

        public Node(Node parent, int height)
        {
            this.parent = parent;
            this.height = height;
        }

        public Node(Node parent, Page page)
        {
            this.parent = parent;
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
        int width = height * RADIX_TREE_NODE_SHIFT;
        int shift = 64 - width; // long = 64 bits

        if (shift < 0)
            return ~0L;

        if (shift >= 64)
            return 0L;

        return ~0L >>> shift;
    }
}
