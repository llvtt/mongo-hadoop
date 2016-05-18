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

import java.io.IOException;
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

    static class GridFSRecordReader extends RecordReader<NullWritable, Text> {

        private GridFSSplit split;
        private Pattern delimiterPattern;
        private Matcher matcher;
        private boolean keepDelimiter;
        private final Text text = new Text();
        private String chunkData;
        private int previousMatchIndex = 0;
        private int totalMatches = 0;

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context)
          throws IOException, InterruptedException {
            this.split = (GridFSSplit) split;
            Configuration conf = context.getConfiguration();

            String patternString =
              MongoConfigUtil.getGridFSDelimiterPattern(conf);
            keepDelimiter = MongoConfigUtil.isGridFSKeepDelimiter(conf);
            chunkData = this.split.getData();
            if (!patternString.isEmpty()) {
                delimiterPattern = Pattern.compile(patternString);
                matcher = delimiterPattern.matcher(chunkData);
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            // No delimiter being used, and we haven't returned anything yet.
            if (null == matcher && 0 == totalMatches) {
                text.set(chunkData);
                ++totalMatches;
                return true;
            } else if (null == matcher) {
                return false;
            }

            // Delimiter used; do we have more matches?
            boolean hasNext = matcher.find();
            if (hasNext) {
                int currentMatchIndex =
                  keepDelimiter ? matcher.end() : matcher.start();
                String token = chunkData.substring(
                  previousMatchIndex, currentMatchIndex);
                previousMatchIndex = currentMatchIndex;
                text.set(token);
                ++totalMatches;
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Read " + totalMatches + " segments.");
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
            return previousMatchIndex / (float) split.getLength();
        }

        @Override
        public void close() throws IOException {
            // Nothing to do.
        }
    }
}
