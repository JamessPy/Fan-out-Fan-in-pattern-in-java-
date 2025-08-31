package fan_out_fan_in;

import java.nio.file.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

public class Fanout {
    // BYTES
    static class Range {
        final long start; // inclusive
        final long end;   // exclusive
        Range(long s, long e) { start = s; end = e; }
        @Override public String toString() { return "[" + start + "," + end + ")"; }
        long length() { return end - start; }
    }

    //	PARTIAL RESULTS
    static class PartialResult {
        final int index;   
        final Range range; 
        final long warn;
        final long error;
        PartialResult(int index, Range range, long warn, long error) {
            this.index = index; this.range = range; this.warn = warn; this.error = error;
        }
    }

    public static void main(String[] args) throws Exception {
    	
    	// READ FILE
        Path file = Paths.get(args.length > 0 ? args[0] : "resources/log.txt");
        if (!Files.exists(file)) {
            System.err.println("File doesn't exist: " + file.toAbsolutePath());
            System.exit(1);
        }

        // PARAMETERS
        int desiredSplits = 12;                 // desired splits count
        long minChunkBytes = 64L * 1024;       //  min split size . 64KB
        int scanWindow = 256 * 1024;           // '\n' search window
        int readBuffer = 256 * 1024;           // I/O

        // Get time
        Instant t0 = Instant.now();

        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = ch.size();
            long targetChunk = Math.max(minChunkBytes, Math.max(1, size / Math.max(1, desiredSplits)));

            // Calculate interval
            List<Range> ranges = computeRangesAlignedToNewline(ch, size, targetChunk, scanWindow);

            // Fan-out: one job per Range (Callable<PartialResult>)
            int workers = Math.min(Runtime.getRuntime().availableProcessors(), ranges.size());
            ExecutorService pool = Executors.newFixedThreadPool(Math.max(2, workers));
            List<Future<PartialResult>> futures = new ArrayList<>();

            for (int i = 0; i < ranges.size(); i++) {
                final int idx = i;
                final Range r = ranges.get(i);
                futures.add(pool.submit(() -> processRangeCountWarnError(ch, r, readBuffer, idx)));
            }
            futures.clear(); 

            for (int i = 0; i < ranges.size(); i++) {
                final int idx = i;
                final Range r = ranges.get(i);
                futures.add(pool.submit(() -> processRangeCountWarnError(ch, r, readBuffer, idx)));
            }

            // Fan-in: collect all partial results
            List<PartialResult> partials = new ArrayList<>();
            for (Future<PartialResult> f : futures) {
                partials.add(f.get()); // Get results when the job done
            }
            pool.shutdown();

            // Debugging partial results
            partials.sort(Comparator.comparingInt(p -> p.index));

            System.out.println("=== Fan-out / Fan-in (paralel) ===");
            System.out.println("File          : " + file.toAbsolutePath());
            System.out.println("Size          : " + size + " bytes");
            System.out.println("Split Count   : " + ranges.size());
            System.out.println("Workers : " + Math.max(2, workers));
            System.out.println();

            long totalWarn = 0, totalError = 0;
            System.out.printf("%-8s %-28s %-12s %-12s %-12s%n",
                    "Split", "Byte", "Length", "WARN", "ERROR");
            System.out.println("--------------------------------------------------------------------------------");
            for (PartialResult p : partials) {
                totalWarn  += p.warn;
                totalError += p.error;
                System.out.printf("%-8d %-28s %-12d %-12d %-12d%n",
                        (p.index + 1), p.range.toString(), p.range.length(), p.warn, p.error);
            }

            Instant t1 = Instant.now();
            System.out.println("--------------------------------------------------------------------------------");
            System.out.printf("%-8s %-28s %-12s %-12d %-12d%n", "", "Total", "", totalWarn, totalError);
            System.out.println("\nTime: " + Duration.between(t0, t1).toMillis() + " ms");
        }
    }

   // Splits the file into slices of approximately targetChunk size, aligning the end of each slice to the next.
    static List<Range> computeRangesAlignedToNewline(FileChannel ch, long size, long targetChunk, int scanWindow) throws Exception {
        List<Range> ranges = new ArrayList<>();
        long start = 0;
        while (start < size) {
            long roughEnd = Math.min(start + targetChunk, size);
            long end = alignToNextNewline(ch, roughEnd, size, scanWindow);
            if (end <= start) end = Math.min(start + targetChunk, size);
            ranges.add(new Range(start, end));
            start = end;
        }
        return ranges;
    }

    //	Finds the first '\n' bytes from position roughEnd; otherwise, end of file is returned.
    static long alignToNextNewline(FileChannel ch, long roughEnd, long size, int scanWindow) throws Exception {
        if (roughEnd >= size) return size;
        long pos = roughEnd;
        ByteBuffer buf = ByteBuffer.allocate((int) Math.min(scanWindow, size - pos));
        while (pos < size) {
            buf.clear();
            int read = ch.read(buf, pos);
            if (read <= 0) break;
            buf.flip();
            for (int i = 0; i < read; i++) {
                if (buf.get(i) == (byte) '\n') {
                    return pos + i + 1;
                }
            }
            pos += read;
        }
        return size;
    }

    // Reads the given range line by line, counts WARN/ERROR and returns PartialResult.
    static PartialResult processRangeCountWarnError(FileChannel ch, Range r, int bufSize, int index) throws Exception {
        ByteBuffer buf = ByteBuffer.allocateDirect(bufSize);
        StringBuilder line = new StringBuilder(256);

        long warn = 0, error = 0;
        long offset = r.start;

        while (offset < r.end) {
            buf.clear();
            int toRead = (int) Math.min(buf.capacity(), r.end - offset); // Read only limited part
            buf.limit(toRead);                                           // Set limit

            int n = ch.read(buf, offset);
            if (n <= 0) break;
            buf.flip();

            for (int i = 0; i < n; i++) {
                byte b = buf.get(i);
                if (b == (byte) '\n') {
                    int inc = classify(line);
                    if (inc == 1) warn++;
                    else if (inc == 2) error++;
                    line.setLength(0);
                } else if (b != (byte) '\r') {
                    line.append((char) (b & 0xFF));
                }
            }
            offset += n;
        }

        // If the range does not end with '\n', count the remaining lines in the queue.
        if (line.length() > 0) {
            int inc = classify(line);
            if (inc == 1) warn++;
            else if (inc == 2) error++;
        }
        return new PartialResult(index, r, warn, error);
    }


    //	0: Other; 1: WARN; 2: ERROR 
    static int classify(CharSequence line) {
        String s = line.toString();
        if (s.contains(" WARN "))  return 1;
        if (s.contains(" ERROR ")) return 2;
        return 0;
    }
}