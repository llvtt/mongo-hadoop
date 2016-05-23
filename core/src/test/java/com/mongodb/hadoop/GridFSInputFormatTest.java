package com.mongodb.hadoop;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GridFSInputFormatTest extends BaseHadoopTest {

    private static final MongoClient client = new MongoClient();
    private static final GridFSInputFormat inputFormat =
      new GridFSInputFormat();
    private static int readmeBytes, readmeSections;
    private static final GridFSBucket bucket = GridFSBuckets.create(
      client.getDatabase("mongo_hadoop"));
    private static StringBuilder fileContents;
    private static GridFSFile file;

    private static void cleanReadmeFiles() {
        if (file != null) {
            bucket.delete(file.getObjectId());
        }
    }

    @BeforeClass
    public static void setUpClass() throws IOException {
        cleanReadmeFiles();
        // Load text files into GridFS.
        GridFSUploadStream stream = bucket.openUploadStream(
          "README.md",
          // Set small chunk size so we get multiple chunks.
          new GridFSUploadOptions().chunkSizeBytes(1024));
        File readmeFile = new File(PROJECT_HOME, "README.md");

        // Count number of bytes in the README.
        readmeBytes = (int) readmeFile.length();

        // Read the README, preparing to count sections and upload to GridFS.
        fileContents = new StringBuilder();
        BufferedReader reader = new BufferedReader(new FileReader(readmeFile));
        reader.mark(readmeBytes + 1);
        int charsRead;
        do {
            char[] buff = new char[1024];
            charsRead = reader.read(buff);
            if (charsRead > 0) {
                fileContents.append(buff, 0, charsRead);
            }
        } while (charsRead > 0);
        reader.reset();
        // Count number of sections in the README ("## ...").
        readmeSections = Pattern.compile("#+").split(fileContents).length;
        IOUtils.copy(reader, stream);
        stream.close();

        file = bucket.find(new Document("filename", "README.md")).first();
    }

    @AfterClass
    public static void tearDownClass() {
        cleanReadmeFiles();
    }

    private Configuration getConfiguration() {
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(
          conf, "mongodb://localhost:27017/mongo_hadoop.fs");
        MongoConfigUtil.setQuery(
          conf, new BasicDBObject("filename", "README.md"));
        return conf;
    }

    private List<InputSplit> getSplits()
      throws IOException, InterruptedException {
        JobContext context = mock(JobContext.class);
        when(context.getConfiguration()).thenReturn(getConfiguration());
        return inputFormat.getSplits(context);
    }

    @Test
    public void testGetSplits() throws IOException, InterruptedException {
        assertEquals(
          (int) Math.ceil(file.getLength() / (float) file.getChunkSize()),
          getSplits().size());
    }

    @Test
    public void testRecordReader() throws IOException, InterruptedException {
        List<InputSplit> splits = getSplits();
        Configuration conf = getConfiguration();
        // Split README by sections in Markdown.
        MongoConfigUtil.setGridFSDelimiterPattern(conf, "#+");
        TaskAttemptContext context = mock(TaskAttemptContext.class);
        when(context.getConfiguration()).thenReturn(conf);
        int totalSections = 0;
        for (InputSplit split : splits) {
            RecordReader reader = new GridFSInputFormat.GridFSRecordReader();
            reader.initialize(split, context);
            while (reader.nextKeyValue()) {
                ++totalSections;
            }
        }
        assertEquals(readmeSections, totalSections);
    }

    @Test
    public void testRecordReaderNoDelimiter()
      throws IOException, InterruptedException {
        int bufferPosition = 0;
        byte[] buff = new byte[readmeBytes * 2];
        List<InputSplit> splits = getSplits();
        Configuration conf = getConfiguration();
        // Empty delimiter == no delimiter.
        MongoConfigUtil.setGridFSDelimiterPattern(conf, "");
        TaskAttemptContext context = mock(TaskAttemptContext.class);
        when(context.getConfiguration()).thenReturn(conf);
        Text text;
        for (InputSplit split : splits) {
            GridFSInputFormat.GridFSRecordReader reader =
              new GridFSInputFormat.GridFSRecordReader();
            reader.initialize(split, context);
            while (reader.nextKeyValue()) {
                text = reader.getCurrentValue();
                System.arraycopy(
                  text.getBytes(), 0,
                  buff, bufferPosition, text.getLength());
                bufferPosition += text.getLength();
            }
        }
        assertEquals(
          fileContents.toString(), Text.decode(buff, 0, bufferPosition));
    }

}
