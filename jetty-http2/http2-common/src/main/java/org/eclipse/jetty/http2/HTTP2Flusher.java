//
//  ========================================================================
//  Copyright (c) 1995-2018 Mort Bay Consulting Pty. Ltd.
//  ------------------------------------------------------------------------
//  All rights reserved. This program and the accompanying materials
//  are made available under the terms of the Eclipse Public License v1.0
//  and Apache License v2.0 which accompanies this distribution.
//
//      The Eclipse Public License is available at
//      http://www.eclipse.org/legal/epl-v10.html
//
//      The Apache License v2.0 is available at
//      http://www.opensource.org/licenses/apache2.0.php
//
//  You may elect to redistribute this code under either of these licenses.
//  ========================================================================
//

package org.eclipse.jetty.http2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.eclipse.jetty.http2.frames.Frame;
import org.eclipse.jetty.http2.frames.WindowUpdateFrame;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.EofException;
import org.eclipse.jetty.io.WriteFlusher;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.IteratingCallback;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.component.Dumpable;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

public class HTTP2Flusher extends IteratingCallback implements Dumpable
{
    private static final Logger LOG = Log.getLogger(HTTP2Flusher.class);
    private static final ByteBuffer[] EMPTY_BYTE_BUFFERS = new ByteBuffer[0];

    private final Queue<WindowEntry> windows = new ArrayDeque<>();
    private final Deque<Entry> entries = new ArrayDeque<>();
    private final List<Entry> processed = new ArrayList<>();
    private final LinkedHashMap<Integer, StreamHead> pending = new LinkedHashMap<>();
    private final HTTP2Session session;
    private final ByteBufferPool.Lease lease;
    private Throwable terminated;
    private final long writeThreshold = 32 * 1024;
    private StreamHead stalled;

    public HTTP2Flusher(HTTP2Session session)
    {
        this.session = session;
        this.lease = new ByteBufferPool.Lease(session.getGenerator().getByteBufferPool());
    }

    public void window(IStream stream, WindowUpdateFrame frame)
    {
        Throwable closed;
        synchronized (this)
        {
            closed = terminated;
            if (closed == null)
                windows.offer(new WindowEntry(stream, frame));
        }
        // Flush stalled data.
        if (closed == null)
            iterate();
    }

    public boolean prepend(Entry entry)
    {
        Throwable closed;
        synchronized (this)
        {
            closed = terminated;
            if (closed == null)
            {
                entries.offerFirst(entry);
                if (LOG.isDebugEnabled())
                    LOG.debug("Prepended {}, entries={}", entry, entries.size());
            }
        }
        if (closed == null)
            return true;
        closed(entry, closed);
        return false;
    }

    public boolean append(Entry entry)
    {
        Throwable closed;
        synchronized (this)
        {
            closed = terminated;
            if (closed == null)
            {
                entries.offer(entry);
                if (LOG.isDebugEnabled())
                    LOG.debug("Appended {}, entries={}", entry, entries.size());
            }
        }
        if (closed == null)
            return true;
        closed(entry, closed);
        return false;
    }

    private int getWindowQueueSize()
    {
        synchronized (this)
        {
            return windows.size();
        }
    }

    public int getFrameQueueSize()
    {
        synchronized (this)
        {
            return entries.size();
        }
    }

    @Override
    protected Action process() throws Throwable
    {
        if (LOG.isDebugEnabled())
            LOG.debug("Flushing {}", session);
        
        synchronized (this)
        {
            if (terminated != null)
                throw terminated;

            WindowEntry windowEntry;
            while ((windowEntry = windows.poll()) != null)
                windowEntry.perform();

            Entry entry;
            while ((entry = entries.poll()) != null)
            {
                IStream stream = entry.stream;
                Integer streamId = stream==null?0:stream.getId();
                StreamHead head = pending.computeIfAbsent(streamId,id->new StreamHead(stream));
                head.entries.add(entry);
            }
        }

        if (pending.isEmpty())
        {
            if (LOG.isDebugEnabled())
                LOG.debug("Flushed {}", session);
            return Action.IDLE;
        }

        outer: while (true)
        {
            boolean progress = false;

            for (Iterator<StreamHead> i = pending.values().iterator(); i.hasNext();)
            {
                StreamHead head = i.next();
                
                // Skip over any data frames to any stalled data frame.
                if (stalled!=null)
                {
                    while (head!=stalled && i.hasNext() && head.stream!=null)
                        head = i.next();
                    stalled = null;
                }

                // Process entries
                while(!head.entries.isEmpty())
                {
                    Entry entry = head.entries.peek();
                    if (LOG.isDebugEnabled())
                        LOG.debug("Processing {}/{}", head, entry);

                    // If the stream has been reset or removed,
                    // don't send the frame and fail it here.
                    if (entry.isStale())
                    {
                        if (LOG.isDebugEnabled())
                            LOG.debug("Stale {}", entry);
                        // TODO move this code to StreamHead
                        EofException eof = new EofException("reset");
                        head.entries.forEach(e->e.failed(eof));
                        head.entries.clear();
                        i.remove();
                        break;
                    }

                    try
                    {
                        if (entry.generate(lease))
                        {
                            if (LOG.isDebugEnabled())
                                LOG.debug("Generated data/frame {}/{} bytes for {}", entry.getDataBytesGenerated(), entry.getFrameBytesGenerated(), entry);
                            
                            progress = true;
                            processed.add(entry);
                            
                            if (entry.getDataBytesRemaining() == 0)
                            {
                                head.entries.removeFirst();
                                if (head.entries.isEmpty())
                                    i.remove(); // TODO should we leave and reuse?
                            }

                            // Only try a single data entry per iteration
                            if (entry.isData())
                                break;
                            
                            if (lease.getTotalLength() >= writeThreshold)
                            {
                                if (LOG.isDebugEnabled())
                                    LOG.debug("Write threshold exceeded {}>{}", lease.getTotalLength(), writeThreshold);
                                if (stalled==null)
                                    stalled = i.hasNext()?i.next():null;
                                break outer;
                            }
                        }
                        else
                        {
                            if (LOG.isDebugEnabled())
                                LOG.debug("Stalled {}/{}", head, entry);

                            if (stalled==null)
                                stalled=head;
                        }
                    }
                    catch (Throwable failure)
                    {
                        // Failure to generate the entry is catastrophic.
                        if (LOG.isDebugEnabled())
                            LOG.debug("Failure generating " + entry, failure);
                        failed(failure);
                        return Action.SUCCEEDED;
                    }
                }
            }

            if (!progress)
                break;
        }

        List<ByteBuffer> byteBuffers = lease.getByteBuffers();
        if (byteBuffers.isEmpty())
        {
            finish();
            return Action.IDLE;
        }

        if (LOG.isDebugEnabled())
            LOG.debug("Writing {} buffers ({} bytes) - entries processed/pending {}/{}: {}/{}",
                    byteBuffers.size(),
                    lease.getTotalLength(),
                    processed.size(),
                    pending.size(),
                    processed,
                    pending);
        session.getEndPoint().write(this, byteBuffers.toArray(EMPTY_BYTE_BUFFERS));
        return Action.SCHEDULED;
    }

    void onFlushed(long bytes) throws IOException
    {
        for (Entry entry : processed)
            bytes = onFlushed(bytes, entry);
    }

    private long onFlushed(long bytes, Entry entry) throws IOException
    {
        // For the given flushed bytes, we want to only
        // forward those that belong to data frame content.
        int frameBytesGenerated = entry.getFrameBytesGenerated();
        if (LOG.isDebugEnabled())
            LOG.debug("Flushed {}/{} frame bytes for {}", frameBytesGenerated, bytes, entry);
        if (frameBytesGenerated > 0)
        {
            bytes -= frameBytesGenerated;
            if (entry.isData())
            {
                Object channel = entry.stream.getAttachment();
                if (channel instanceof WriteFlusher.Listener)
                {
                    int dataBytesGenerated = entry.getDataBytesGenerated();
                    if (LOG.isDebugEnabled())
                        LOG.debug("Flushed {} data bytes for {}", dataBytesGenerated, entry);
                    if (dataBytesGenerated > 0)
                        ((WriteFlusher.Listener)channel).onFlushed(dataBytesGenerated);
                }
            }
        }
        return bytes;
    }

    @Override
    public void succeeded()
    {
        if (LOG.isDebugEnabled())
            LOG.debug("Written {} buffers - entries processed/pending {}/{}: {}/{}",
                    lease.getByteBuffers().size(),
                    processed.size(),
                    pending.size(),
                    processed,
                    pending);
        finish();
        super.succeeded();
    }

    private void finish()
    {
        lease.recycle();
        processed.forEach(Entry::succeeded);
        processed.clear();
    }

    @Override
    protected void onCompleteSuccess()
    {
        throw new IllegalStateException();
    }

    @Override
    protected void onCompleteFailure(Throwable x)
    {
        lease.recycle();

        Throwable closed;
        
        Set<Entry> allEntries = new HashSet<>();
        
        synchronized (this)
        {
            closed = terminated;
            terminated = x;
            if (LOG.isDebugEnabled())
                LOG.debug("{}, entries processed/pending/queued={}/{}/{}",
                        closed != null ? "Closing" : "Failing",
                        processed.size(),
                        pending.size(),
                        entries.size());
            allEntries.addAll(entries);
            entries.clear();
        }

        allEntries.addAll(processed);
        pending.values().forEach(head -> allEntries.addAll(head.entries));
        pending.clear();
        
        allEntries.forEach(entry -> entry.failed(x));

        // If the failure came from within the
        // flusher, we need to close the connection.
        if (closed == null)
            session.abort(x);
    }

    void terminate(Throwable cause)
    {
        Throwable closed;
        synchronized (this)
        {
            closed = terminated;
            terminated = cause;
            if (LOG.isDebugEnabled())
                LOG.debug("{}", closed != null ? "Terminated" : "Terminating");
        }
        if (closed == null)
            iterate();
    }

    private void closed(Entry entry, Throwable failure)
    {
        entry.failed(failure);
    }

    @Override
    public String dump()
    {
        return ContainerLifeCycle.dump(this);
    }

    @Override
    public void dump(Appendable out, String indent) throws IOException
    {
        out.append(toString()).append(System.lineSeparator()).append(indent);
    }

    @Override
    public String toString()
    {
        return String.format("%s[window_queue=%d,frame_queue=%d,completed/pending=%d/%d]",
                super.toString(),
                getWindowQueueSize(),
                getFrameQueueSize(),
                processed.size(),
                pending.size());
    }

    public static class StreamHead
    {
        private final IStream stream;
        private final Deque<Entry> entries = new ArrayDeque<>();
        
        private StreamHead(IStream stream)
        {
            this.stream = stream;
        }
        
        @Override 
        public String toString()
        {
            return String.format("StreamHead%x{%d,q=%d}",hashCode(),stream==null?0:stream.getId(),entries.size());
        }
    }
    
    public static abstract class Entry extends Callback.Nested
    {
        protected final Frame frame;
        protected final IStream stream;

        protected Entry(Frame frame, IStream stream, Callback callback)
        {
            super(callback);
            this.frame = frame;
            this.stream = stream;
        }

        public abstract int getFrameBytesGenerated();

        public int getDataBytesGenerated()
        {
            return 0;
        }

        public int getDataBytesRemaining()
        {
            return 0;
        }

        protected abstract boolean generate(ByteBufferPool.Lease lease);

/*
        private void finish()
        {
            if (isStale())
                failed(new EofException("reset"));
            else
                succeeded();
        }
*/

        @Override
        public void failed(Throwable x)
        {
            if (stream != null)
            {
                stream.close();
                stream.getSession().removeStream(stream);
            }
            super.failed(x);
        }

        private boolean isStale()
        {
            return !isProtocol() && stream != null && stream.isReset();
        }

        private boolean isProtocol()
        {
            switch (frame.getType())
            {
                case DATA:
                case HEADERS:
                case PUSH_PROMISE:
                case CONTINUATION:
                    return false;
                case PRIORITY:
                case RST_STREAM:
                case SETTINGS:
                case PING:
                case GO_AWAY:
                case WINDOW_UPDATE:
                case PREFACE:
                case DISCONNECT:
                    return true;
                default:
                    throw new IllegalStateException();
            }
        }

        private boolean isData()
        {
            switch (frame.getType())
            {
                case DATA:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public String toString()
        {
            return frame.toString();
        }
    }

    private class WindowEntry
    {
        private final IStream stream;
        private final WindowUpdateFrame frame;

        public WindowEntry(IStream stream, WindowUpdateFrame frame)
        {
            this.stream = stream;
            this.frame = frame;
        }

        public void perform()
        {
            FlowControlStrategy flowControl = session.getFlowControlStrategy();
            flowControl.onWindowUpdate(session, stream, frame);
        }
    }
}
