package com.globalpayments.batch.pipeline.utils;


import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.util.ExposedByteArrayOutputStream;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;
//import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Utf8;
//import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import com.google.common.base.Utf8;
import com.google.common.io.ByteStreams;


import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * A {@link Coder} that encodes {@link String Strings} in ASCII encoding. If in a nested context,
 * prefixes the string with an integer length field, encoded via a {@link VarIntCoder}.
 */
public class StringAsciiCoder extends AtomicCoder<String> {
    private static final long serialVersionUID = -5311769689667125702L;

    public static StringAsciiCoder of() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////////////

    private static final StringAsciiCoder INSTANCE = new StringAsciiCoder();
    private static final TypeDescriptor<String> TYPE_DESCRIPTOR = new TypeDescriptor<String>() {

        private static final long serialVersionUID = 8736460447371742811L;};

    private static void writeString(String value, OutputStream dos) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        VarInt.encode(bytes.length, dos);
        dos.write(bytes);
    }

    private static String readString(InputStream dis) throws IOException {
        int len = VarInt.decodeInt(dis);
        if (len < 0) {
            throw new CoderException("Invalid encoded string length: " + len);
        }
        byte[] bytes = new byte[len];
        ByteStreams.readFully(dis, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private StringAsciiCoder() {}

    @Override
    public void encode(String value, OutputStream outStream) throws IOException {
        encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(String value, OutputStream outStream, Context context) throws IOException {
        if (value == null) {
            throw new CoderException("cannot encode a null String");
        }
        if (context.isWholeStream) {
            byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
            if (outStream instanceof ExposedByteArrayOutputStream) {
                ((ExposedByteArrayOutputStream) outStream).writeAndOwn(bytes);
            } else {
                outStream.write(bytes);
            }
        } else {
            writeString(value, outStream);
        }
    }

    @Override
    public String decode(InputStream inStream) throws IOException {
        return decode(inStream, Context.NESTED);
    }

    @Override
    public String decode(InputStream inStream, Context context) throws IOException {
        if (context.isWholeStream) {
            byte[] bytes = StreamUtils.getBytesWithoutClosing(inStream);
            return new String(bytes, StandardCharsets.UTF_8);
        } else {
            try {
                return readString(inStream);
            } catch (EOFException | UTFDataFormatException exn) {
                // These exceptions correspond to decoding problems, so change
                // what kind of exception they're branded as.
                throw new CoderException(exn);
            }
        }
    }

    @Override
    public void verifyDeterministic() {}

    /**
     * {@inheritDoc}
     *
     * @return {@code true}. This coder is injective.
     */
    @Override
    public boolean consistentWithEquals() {
        return true;
    }

    @Override
    public TypeDescriptor<String> getEncodedTypeDescriptor() {
        return TYPE_DESCRIPTOR;
    }

    /**
     * {@inheritDoc}
     *
     * @return the byte size of the UTF-8 encoding of the a string or, in a nested context, the byte
     *     size of the encoding plus the encoded length prefix.
     */
    @Override
    public long getEncodedElementByteSize(String value) throws Exception {
        if (value == null) {
            throw new CoderException("cannot encode a null String");
        }
        int size = Utf8.encodedLength(value);
        return (long) VarInt.getLength(size) + size;
    }
}


