package com.github.prologdb.storage.heapfile

data class HeapFileHeader(
    /** Size of the individual pages, in bytes */
    val pageSize: Int,

    /**
     * Number of bytes following the header that attempt to
     * align the heap file pages with physical disk blocks
     */
    val alignmentPaddingSize: Int
)