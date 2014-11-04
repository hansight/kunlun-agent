package com.hansight.kunlun.collector.utils;

import org.apache.hadoop.fs.Seekable;
import org.mozilla.universalchardet.UniversalDetector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

public final class FileUtils {


    private FileUtils() {
    }

    /**
     * reader charset detect,some known problem is that:
     * because detect is base guess, when give information is much less, result is  none,then we thinks win is GBK, other is utf-8
     *
     * @param reader
     * @return
     * @throws Exception
     */
    public static String lineSeparator(BufferedReader reader) throws IOException {
        if (reader.markSupported()) {
            reader.mark(1024 * 8);
        }
        String line = reader.readLine();
        if (line == null) {
            reset(reader);
            return "";
        }

        char[] chars = new char[line.length()];
        reset(reader);
        reader.read(chars);
        chars = new char[3];
        reader.read(chars);
        reset(reader);
        reader.readLine();
        line = reader.readLine();
        reset(reader);
        if (line == null) {
            return "";
        }
        char start = line.charAt(0);
        int i = 0;
        for (; i < 3; i++) {
            if (start == chars[i]) {
                break;
            }
        }
        char[] chars1 = new char[i];
        int j = 0;
        while (j < i) {
            chars1[j] = chars[j];
            j++;
        }


        return new String(chars1);
    }

    private static void reset(BufferedReader reader) throws IOException {
        if (reader.markSupported()) {
            reader.reset();
        }
        if (reader instanceof Seekable) {
            ((Seekable) reader).seek(0);
        }
    }

    private final static UniversalDetector detector = new UniversalDetector(null);

    /**
     * reader charset detect,some known problem is that:
     * because detect is base guess, when give information is much less, result is  none,then we thinks win is GBK, other is utf-8
     *
     * @param reader
     * @return
     * @throws Exception
     */
    public static String charset(InputStream reader) throws IOException {
        String encoding = null;
        byte[] buf = new byte[4096];
        if (reader.markSupported()) {
            reader.mark(1024 * 8);
        }

        int len = reader.read(buf);
        if (len > 0) {
            detector.handleData(buf, 0, len);
            detector.dataEnd();
            encoding = detector.getDetectedCharset();
        }
        if (encoding == null) {
            String os = System.getProperty("os.name");
            if (os.toLowerCase().contains("windows"))
                encoding = "GB18030";
            else {
                encoding = "UTF-8";
            }
        }
        if (reader.markSupported()) {
            reader.reset();
        }
        if (reader instanceof Seekable) {
            ((Seekable) reader).seek(0);
        }
        return encoding;
    }

    /**
     * simple reader charset detect,some known problem is UTF-8 no BOM cannot be detect ,will return GBK
     *
     * @param reader
     * @return
     * @throws java.io.IOException
     */
    public static String simpleCharset(InputStream reader) throws IOException {
        int high = reader.read();
        int low = reader.read();
        int p = (high << 8) + low;
        String code;
        //其中的 0xefbb、0xfffe、0xfeff、0x5c75这些都是这个文件的前面两个字节的16进制数
        switch (p) {
            case 0xefbb:
                code = "UTF-8";
                break;
            case 0xfffe:
                code = "UTF-16LE";
                break;
            case 0xfeff:
                code = "UTF-16BE";
                break;
            case 0x5c75:
                code = "ASCII";
                break;
            default:
                code = "GBK";
        }

        return code;
    }

}
