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

import com.mongodb.MongoException;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class MongoRecordReader implements RecordReader<BSONWritable, BSONWritable> {

    private static final Log LOG = LogFactory.getLog(MongoRecordReader.class);
    private final com.mongodb.hadoop.input.MongoRecordReader delegate;
    
    private BSONWritable currentVal = new BSONWritable();
    private BSONWritable currentKey = new BSONWritable();

    public MongoRecordReader(final MongoInputSplit split) {
        delegate = new com.mongodb.hadoop.input.MongoRecordReader(split);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public BSONWritable createKey() {
        return new BSONWritable();
    }

    @Override
    public BSONWritable createValue() {
        return new BSONWritable();
    }

    /**
     * Get the key for the most recent document iterated by this RecordReader.
     * @return a BSONWritable with a document of the form
     * <code>{"_id": (current document id)}</code>, but
     * <em>this will change</em> in the future in order to support
     * selecting what field to use (not just _id) as the input key.
     * {@link #next(BSONWritable, BSONWritable)} instead.
     */
    public BSONWritable getCurrentKey() {
        return this.currentKey;
    }

    /**
     * Get the whole document most recently iterated by this RecordReader.
     * @return a BSONWritable containing the most recently iterated document.
     * @deprecated use the value in the second parameter of
     * {@link #next(BSONWritable, BSONWritable)} instead.
     */
    public BSONWritable getCurrentValue() {
        return this.currentVal;
    }

    @Override
    public float getProgress() {
        return delegate.getProgress();
    }

    @Override
    public long getPos() {
        return 0; // no progress to be reported, just working on it
    }

    public void initialize(
      final InputSplit split, final TaskAttemptContext context) {
        delegate.initialize(null, null);
    }

    public boolean nextKeyValue() throws IOException {
        try {
            if (!delegate.nextKeyValue()) {
                return false;
            }

            BSONObject document = delegate.getCurrentValue();
            this.currentVal.setDoc(document);
            this.currentKey.setDoc(
              new BasicBSONObject("_id", document.get("_id")));

            return true;
        } catch (MongoException e) {
            throw new IOException("Couldn't get next key/value from mongodb: ", e);
        }
    }

    /**
     * Get the next key and value.
     * @param key a BSONWritable that will be filled with the next key. The key
     *            is always of the form <code>{"_id": (document _id)}</code>,
     *            but <em>this will change</em> in the future in order to support
     *            selecting what field to use (not just _id) as the input key.
     * @param value a BSONWritable that will be filled with the next entire
     *              document.
     * @return <code>true</code> if there's a next document to be retrieved,
     * <code>false</code> otherwise.
     * @throws IOException
     */
    @Override
    public boolean next(final BSONWritable key, final BSONWritable value)
      throws IOException {
        if (nextKeyValue()) {
            key.setDoc(this.currentKey.getDoc());
            value.setDoc(this.currentVal.getDoc());
            return true;
        } else {
            LOG.info("Cursor exhausted.");
            return false;
        }
    }
}
