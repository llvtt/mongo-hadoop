package com.mongodb.hadoop.splitter;

import com.mongodb.CommandResult;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class ShardChunkMongoSplitterTest extends BaseHadoopTest {

    private ShardChunkMongoSplitter splitter = new ShardChunkMongoSplitter();
    private static MongoClientURI inputURI;
    private static MongoClient client;
    private static Map<String, String> shardsMap;

    @BeforeClass
    public static void setUpClass() {
        inputURI = new MongoClientURI("mongodb://localhost:27017/mongo_hadoop.splitter_test");
        client = new MongoClient(inputURI);

        // Build a map of shard name -> set of shard hosts.
        shardsMap = new HashMap<String, String>();
        Document listShards = client.getDatabase("admin").runCommand(
          new Document("listShards", 1));
        for (Object shardObj : (List) listShards.get("shards")) {
            Document shard = (Document) shardObj;
            shardsMap.put(shard.getString("_id"), shard.getString("host"));
        }

        // Set up some initial data and make sure it's split across shards.
        MongoCollection<Document> testCollection =
          client.getDatabase("mongo_hadoop").getCollection("splitter_test");
        List<Document> testData = new ArrayList<Document>(1000);
        for (int i = 0; i < 1000; ++i) {
            testData.add(new Document("i", i));
        }
        testCollection.insertMany(testData);
        testCollection.createIndex(new Document("i", 1));

        MongoDatabase adminDatabase = client.getDatabase("admin");

        adminDatabase.runCommand(new Document("enableSharding", "mongo_hadoop"));
        adminDatabase.runCommand(
          new Document("shardCollection", "mongo_hadoop.splitter_test")
            .append("key", new Document("i", 1)));

        // Split collection in half.
        adminDatabase.runCommand(
          new Document("split", "mongo_hadoop.splitter_test")
            .append("middle", new Document("i", 500)));
        Iterator<String> shardsIterator = shardsMap.keySet().iterator();
        String firstShard = shardsIterator.next();
        try {
            adminDatabase.runCommand(
              new Document("moveChunk", "mongo_hadoop.splitter_test")
                .append("find", new Document("i", 1))
                .append("to", firstShard));
        } catch (MongoCommandException e) {
            // Chunk may already be on that shard.
        }
        String secondShard = shardsIterator.next();
        try {
            adminDatabase.runCommand(
              new Document("moveChunk", "mongo_hadoop.splitter_test")
                .append("find", new Document("i", 999))
                .append("to", secondShard));
        } catch (MongoCommandException e) {
            // Chunk may already be on that shard.
        }
    }

    @AfterClass
    public static void tearDownClass() {
        client.dropDatabase("mongo_hadoop");
    }

    @Test
    public void testSplitPreferredLocations()
      throws SplitFailedException, IOException, InterruptedException {
        assumeTrue(isSharded(inputURI));

        // Build a map of split min/max -> shard host.
        MongoCollection<Document> chunksCollection =
          client.getDatabase("config").getCollection("chunks");
        Map<MinMaxPair, String> chunksMap = new HashMap<MinMaxPair, String>();
        for (Document chunk : chunksCollection.find()) {
            chunksMap.put(
              new MinMaxPair(chunk.get("min"), chunk.get("max")),
              shardsMap.get(chunk.getString("shard")));
        }

        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(conf, inputURI);
        splitter.setConfiguration(conf);

        List<InputSplit> splits = splitter.calculateSplits();

        // Use the map to assert that each split has the correct preferred location.
        for (InputSplit split : splits) {
            MongoInputSplit mis = (MongoInputSplit) split;
            assertEquals(1, split.getLocations().length);
            MinMaxPair splitMinMax = new MinMaxPair(mis.getMin(), mis.getMax());
            String mappedLocation = chunksMap.get(splitMinMax);
            assertEquals(mappedLocation, mis.getLocations()[0]);
        }
    }

    private class MinMaxPair {
        private Object min, max;

        MinMaxPair(final Object min, final Object max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public int hashCode() {
            return this.min.hashCode() ^ this.max.hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            if (other instanceof MinMaxPair) {
                MinMaxPair otherPair = (MinMaxPair) other;
                return otherPair.min.equals(this.min) && otherPair.max.equals(this.max);
            }
            return false;
        }
    }

}
