package io.windmill.disk;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class PageRef
{
    private final File file;
    private final int pageOffset;

    public PageRef(File file, int pageOffset)
    {
        this.file = file;
        this.pageOffset = pageOffset;
    }

    public void evict()
    {
        file.evictPage(pageOffset);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder().append(file).append(pageOffset).build();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null || !(other instanceof PageRef))
            return false;

        PageRef o = (PageRef) other;
        return file == o.file && pageOffset == o.pageOffset;
    }
}
