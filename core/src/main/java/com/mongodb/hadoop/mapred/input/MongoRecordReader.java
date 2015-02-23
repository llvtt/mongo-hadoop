/*
 * Copyright 2010-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.mapred.input;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.bson.BasicBSONObject;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class MongoRecordReader implements RecordReader<BSONWritable, BSONWritable> {

    private static final Log LOG = LogFactory.getLog(MongoRecordReader.class);
    private final com.mongodb.hadoop.input.MongoRecordReader delegate;
    
    private MongoInputSplit split;

    public MongoRecordReader(final MongoInputSplit split) {
        delegate = new com.mongodb.hadoop.input.MongoRecordReader(split);
    }

    public void close() {
        delegate.close();
    }


    public BSONWritable createKey() {
        return new BSONWritable();
    }

    public BSONWritable createValue() {
        return new BSONWritable();
    }

    public BSONWritable getCurrentKey() {
        // TODO: support keys that aren't DBObjects, just like we already
        // do in the Hadoop 2.0 MongoRecordReader.
        return new BSONWritable(
                new BasicDBObject("_id", delegate.getCurrentKey())
        );
    }

    public BSONWritable getCurrentValue() {
        return new BSONWritable(delegate.getCurrentValue());
    }

    public float getProgress() {
        return delegate.getProgress();
    }

    public long getPos() {
        return 0; // no progress to be reported, just working on it
    }

    public void initialize(final InputSplit split, final TaskAttemptContext context) {
        // delegate doesn't use either of these arguments, so no point in
        // massaging our arguments to fit the Hadoop 2.0 API.
        delegate.initialize(null, null);
    }

    public boolean nextKeyValue() throws IOException {
        return delegate.nextKeyValue();
    }

    public boolean next(final BSONWritable key, final BSONWritable value) throws IOException {
        if (nextKeyValue()) {
            // TODO: support keys that aren't DBObjects, just like we already
            // do in the Hadoop 2.0 MongoRecordReader.
            BasicBSONObject keyDoc = new BasicBSONObject(
                    "_id", delegate .getCurrentKey());
            key.setDoc(keyDoc);
            value.setDoc(delegate.getCurrentValue());
            return true;
        } else {
            LOG.info("Cursor exhausted.");
            return false;
        }
    }
}
