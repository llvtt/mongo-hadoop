package com.mongodb.hadoop.examples.shakespeare;

import com.mongodb.hadoop.GridFSInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.input.GridFSSplit;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

/**
 * MapReduce job that counts the most common exclamations in his complete works.
 */
public class Shakespeare extends MongoTool {
    public Shakespeare() {
        JobConf conf = new JobConf(new Configuration());
        if (MongoTool.isMapRedV1()) {
            // TODO
        } else {
            MongoConfigUtil.setInputFormat(conf, GridFSInputFormat.class);
            MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);
        }
        // Regex: lookahead for "end of sentence" punctuation.
        MongoConfigUtil.setGridFSDelimiterPattern(conf, "(?<=[.?!])");
        MongoConfigUtil.setMapper(conf, ShakespeareMapper.class);
        MongoConfigUtil.setMapperOutputKey(conf, ShakespeareWritable.class);
        MongoConfigUtil.setMapperOutputValue(conf, IntWritable.class);
        MongoConfigUtil.setReducer(conf, ShakespeareReducer.class);
        MongoConfigUtil.setOutputKey(conf, NullWritable.class);
        MongoConfigUtil.setOutputValue(conf, BSONWritable.class);
        MongoConfigUtil.setOutputURI(
          conf,
          "mongodb://localhost:27017/mongo_hadoop.shakespeare.out");
        setConf(conf);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Shakespeare(), args));
    }

    class ShakespeareWritable implements Writable {
        private String workTitle;
        private String exclamation;

        public String getExclamation() {
            return exclamation;
        }

        public void setExclamation(final String exclamation) {
            this.exclamation = exclamation;
        }

        public String getWorkTitle() {
            return workTitle;
        }

        public void setWorkTitle(final String workTitle) {
            this.workTitle = workTitle;
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            out.writeUTF(workTitle);
            out.writeUTF(exclamation);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            workTitle = in.readUTF();
            exclamation = in.readUTF();
        }
    }

    class ShakespeareMapper
      extends Mapper<NullWritable, Text, ShakespeareWritable, IntWritable> {
        final HashSet<String> secondPersonPronouns = new HashSet<String>() {{
            add("you");
            add("your");
            add("yours");
            add("ye");
            add("thou");
            add("thy");
            add("thine");
            add("thee");
        }};
        final int MAX_EXCLAMATION_WORDS = 3;
        final ShakespeareWritable sw = new ShakespeareWritable();
        final IntWritable intWritable = new IntWritable();

        private boolean isExclamation(final String test) {
            // Exclamations must end!
            if (!test.endsWith("!")) {
                return false;
            }
            String[] words = test.split(" \\+");
            // We figure the most entertaining exclamations will be directed at
            // the listener.
            for (String word : words) {
                if (secondPersonPronouns.contains(word)) {
                    return true;
                }
            }
            // Exclamations be brief!
            return words.length <= MAX_EXCLAMATION_WORDS;
        }

        @Override
        protected void map(final NullWritable key, final Text value, final Context context)
          throws IOException, InterruptedException {
            GridFSSplit gridSplit = (GridFSSplit) context.getInputSplit();

            // Work title will become the output key.
            String workTitle = (String) gridSplit.get("filename");

            // Extract exclamations.
            String sentence = Text.decode(value.getBytes());
            if (isExclamation(sentence)) {
                sw.setWorkTitle(workTitle);
                sw.setExclamation(sentence);
                intWritable.set(1);
                context.write(sw, intWritable);
            }
        }
    }

    class ShakespeareReducer
      extends Reducer<ShakespeareWritable, IntWritable, NullWritable,
      BSONWritable> {
        final BSONWritable bsonWritable = new BSONWritable();

        @Override
        protected void reduce(
          final ShakespeareWritable key, final Iterable<IntWritable> values,
          final Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            BSONObject result = new BasicBSONObject("finalCount", count);
            result.put("exclamation", key.getExclamation());
            result.put("work", key.getWorkTitle());
            bsonWritable.setDoc(result);
            context.write(NullWritable.get(), bsonWritable);
        }
    }
}
