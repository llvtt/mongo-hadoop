package com.mongodb.hadoop;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.hadoop.input.GridFSSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.types.ObjectId;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GridFSInputFormat extends InputFormat<NullWritable, Text> {

    private static final Log LOG = LogFactory.getLog(GridFSInputFormat.class);

    @Override
    public List<InputSplit> getSplits(final JobContext context)
      throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        DBCollection inputCollection =
          MongoConfigUtil.getInputCollection(conf);
        MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);

        // One split per file chunk.
        GridFS gridFS = new GridFS(
          inputCollection.getDB(),
          inputCollection.getName());

        DBObject query = MongoConfigUtil.getQuery(conf);
        List<InputSplit> splits = new LinkedList<InputSplit>();

        for (GridFSDBFile file : gridFS.find(query)) {
            for (int chunk = 0; chunk < file.numChunks(); ++chunk) {
                splits.add(
                  new GridFSSplit(
                    inputURI,
                    (ObjectId) file.getId(),
                    (int) file.getChunkSize(),
                    file.getLength(),
                    chunk));
            }
        }

        LOG.debug("Found GridFS splits: " + splits);
        return splits;
    }

    @Override
    public RecordReader<NullWritable, Text>
    createRecordReader(final InputSplit split, final TaskAttemptContext context)
      throws IOException, InterruptedException {
        return new GridFSRecordReader();
    }

    static class ChunkReadingCharSequence implements CharSequence, Closeable {
        private Reader reader;
        private int chunkSize;
        private int length;
        private StringBuilder builder;
        private char[] buff;

        public ChunkReadingCharSequence(final GridFSSplit split)
          throws IOException {
            this.reader = new BufferedReader(
              new InputStreamReader(split.getData()));
            this.chunkSize = split.getChunkSize();
            builder = new StringBuilder();
            buff = new char[1024 * 1024 * 16];
            // How many more bytes can be read starting from this chunk?
            length = (int) split.getLength() - split.getChunkId() * chunkSize;
        }

        @Override
        public int length() {
            return length;
        }

        private void advanceToIndex(final int index) throws IOException {
            if (index >= builder.length()) {
                while (index >= builder.length()) {
                    int bytesRead = reader.read(buff);
                    if (bytesRead > 0) {
                        builder.append(buff, 0, bytesRead);
                    } else {
                        break;
                    }
                }
            }
        }

        @Override
        public char charAt(final int index) {
            try {
                advanceToIndex(index);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return builder.charAt(index);
        }

        @Override
        public CharSequence subSequence(final int start, final int end) {
            try {
                advanceToIndex(end);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return builder.subSequence(start, end);
        }

        /**
         * Get the entire contents of this GridFS chunk.
         * @return the contents of the chunk as a CharSequence (a String).
         */
        public CharSequence chunkContents() {
            return subSequence(0, Math.min(chunkSize, length()));
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    static class GridFSRecordReader extends RecordReader<NullWritable, Text> {

        private GridFSSplit split;
        private final Text text = new Text();
        private int totalMatches = 0;
        private long chunkSize;
        private boolean readLast = false;
        private Pattern delimiterPattern;
        private Matcher matcher;
        private int previousMatchIndex = 0;
        private ChunkReadingCharSequence chunkData;

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context)
          throws IOException, InterruptedException {
            this.split = (GridFSSplit) split;
            Configuration conf = context.getConfiguration();

            String patternString =
              MongoConfigUtil.getGridFSDelimiterPattern(conf);
            chunkSize = this.split.getChunkSize();
            chunkData = new ChunkReadingCharSequence(this.split);
            if (!(null == patternString || patternString.isEmpty())) {
                delimiterPattern = Pattern.compile(patternString);
                matcher = delimiterPattern.matcher(chunkData);

                // Skip past the first delimiter if this is not the first chunk.
                if (this.split.getChunkId() > 0) {
                    CharSequence skipped = nextToken();
                    LOG.info("skipping past already-read token: " + skipped);
                }
            }
        }

        private CharSequence nextToken() {
            if (matcher.find()) {
                int currentMatchIndex = matcher.start();
                CharSequence slice = chunkData.subSequence(
                  previousMatchIndex, currentMatchIndex);
                previousMatchIndex = currentMatchIndex;
                return slice;
            }
            // Last token after the final delimiter.
            readLast = true;
            return chunkData.subSequence(
              previousMatchIndex, chunkData.length());
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (readLast) {
                LOG.debug("skipping the rest of this chunk because we've "
                    + "read beyond the end: " + previousMatchIndex
                    + "; read " + totalMatches + " matches here.");
                return false;
            }

            // No delimiter being used, and we haven't returned anything yet.
            if (null == matcher) {
                text.set(chunkData.chunkContents().toString());
                ++totalMatches;
                readLast = true;
                return true;
            }

            // Delimiter used; do we have more matches?
            CharSequence nextToken = nextToken();
            if (nextToken != null) {
                // Read one more token past the end of the split.
                if (previousMatchIndex >= chunkSize) {
                    readLast = true;
                }
                text.set(nextToken.toString());
                ++totalMatches;
                return true;
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Read " + totalMatches + " segments.");
            }

            // No match.
            return false;
        }

        @Override
        public NullWritable getCurrentKey()
          throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return text;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) Math.min(
              previousMatchIndex / (float) chunkSize, 1.0);
        }

        @Override
        public void close() throws IOException {
            chunkData.close();
        }
    }
}
