package fan_out_fan_in;


import java.nio.file.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
public class SingleThread {

	public static void main(String[] args) throws Exception {
		
		// READ FILE
        Path file = Paths.get(args.length > 0 ? args[0] : "resources/log.txt");
        if (!Files.exists(file)) {
            System.err.println("File doesn't exist: " + file.toAbsolutePath());
            System.exit(1);
        }

        int readBuffer = 256 * 1024; // I/O

        long warn = 0, error = 0;
        // Get time
        Instant t0 = Instant.now();
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = ch.size();
            ByteBuffer buf = ByteBuffer.allocateDirect(readBuffer);
            StringBuilder line = new StringBuilder(256);

            long offset = 0;
            while (offset < size) {
                buf.clear();
                int n = ch.read(buf, offset);
                if (n < 0) break;
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
            // If the last line does not end with \n
            if (line.length() > 0) {
                int inc = classify(line);
                if (inc == 1) warn++;
                else if (inc == 2) error++;
            }

            Instant t1 = Instant.now();
            System.out.println("=== Single Thread Baseline ===");
            System.out.println("File : " + file.toAbsolutePath());
            System.out.println("Size : " + size + " bytes");
            System.out.println("WARN  : " + warn);
            System.out.println("ERROR : " + error);
            System.out.println("Time  : " + Duration.between(t0, t1).toMillis() + " ms");
        }
    }

    /** 0: Other; 1: WARN; 2: ERROR */
    static int classify(CharSequence line) {
        String s = line.toString();
        if (s.contains(" WARN "))  return 1;
        if (s.contains(" ERROR ")) return 2;
        return 0;
    }
}
