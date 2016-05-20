package com.mongodb.hadoop;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.hadoop.input.GridFSSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.io.input.CountingInputStream;
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

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

    static class GridFSRecordReader extends RecordReader<NullWritable, Text> {

        private GridFSSplit split;
        private Scanner scanner;
        private final Text text = new Text();
        private CountingInputStream chunkStream;
        private int totalMatches = 0;
        private long chunkSize;
        private boolean readLast = false;

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context)
          throws IOException, InterruptedException {
            this.split = (GridFSSplit) split;
            Configuration conf = context.getConfiguration();

            String patternString =
              MongoConfigUtil.getGridFSDelimiterPattern(conf);
            chunkStream = new CountingInputStream(this.split.getData());
            chunkSize = this.split.getChunkSize();
            if (!patternString.isEmpty()) {
                scanner = new Scanner(chunkStream);
                scanner.useDelimiter(patternString);
                // Skip past the first delimiter if this is not the first chunk.
                if (this.split.getChunkId() > 0 && scanner.hasNext()) {
                    LOG.info("skipping past already-read token: " + scanner
                        .next());
                }
            }
        }

        private String getChunkContents() throws IOException {
            StringBuilder builder = new StringBuilder();
            char[] buff = new char[this.split.getChunkSize()];
            BufferedReader reader =
              new BufferedReader(new InputStreamReader(chunkStream));
            int bytesRead;
            do {
                bytesRead = reader.read(buff);
                builder.append(buff, 0, bytesRead);
            } while (bytesRead > 0);
            return builder.toString();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            // No delimiter being used, and we haven't returned anything yet.
            if (null == scanner && 0 == totalMatches) {
                text.set(getChunkContents());
                ++totalMatches;
                return true;
            } else if (null == scanner) {
                return false;
            }

            // Delimiter used; do we have more matches?
            long currentPositionInChunk =
              chunkStream.getByteCount() - (chunkSize * split.getChunkId());
            boolean hasNext = scanner.hasNext();
            if (hasNext) {
                // Read one more token past the end of the split.
                if (currentPositionInChunk > chunkSize) {
                    if (readLast) {
                        LOG.info("skipping the rest of this chunk because we've "
                            + "read beyond the end: " + currentPositionInChunk +
                            "; read " + totalMatches + " matches here.");
                        return false;
                    }
                    readLast = true;
                }
                ////////////////////////////////
                // TODO: debug only!
                String token = scanner.next();
                LOG.info("got token: " + token + "; position=" +
                    currentPositionInChunk + "; total overall=" + chunkStream
                    .getByteCount());
                ////////////////////////////////

                text.set(token);
                ++totalMatches;
            } else if (LOG.isDebugEnabled()) {
                LOG.info("Read " + totalMatches + " segments.");
            }

            return hasNext;
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
              chunkStream.getByteCount() / (float) split.getChunkSize(), 1.0);
        }

        @Override
        public void close() throws IOException {
            if (scanner != null) {
                scanner.close();
            }
        }
    }
}
