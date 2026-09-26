package org.pipelineframework.opencsv;

import org.pipelineframework.paging.PagedSourceOperation;

/** OpenCSV source boundary that can reopen a pinned snapshot from an opaque checkpoint. */
public interface PagedOpenCsvInputBoundary<I, O>
    extends OpenCsvInputBoundary<I, O>, PagedSourceOperation<I, O> {
}
