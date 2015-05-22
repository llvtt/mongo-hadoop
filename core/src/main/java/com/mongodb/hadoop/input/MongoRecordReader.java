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

package com.mongodb.hadoop.input;

import com.mongodb.DBCursor;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoPathRetriever;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.bson.BSONObject;

import java.util.HashMap;

public class MongoRecordReader extends RecordReader<Object, BSONObject> {

    private static final HashMap<MongoClientURI, CountLock> CLIENTS_MAP =
      new HashMap<MongoClientURI, CountLock>();
    private final CountLock lock;
    private static final Log LOG = LogFactory.getLog(MongoRecordReader.class);
    private BSONObject current;
    private final MongoInputSplit split;
    private DBCursor cursor;
    private float seen = 0;

    // DEBUG only
    TaskAttemptID tid;

    public MongoRecordReader(final MongoInputSplit split) {
        this.split = split;

        synchronized (CLIENTS_MAP) {
            MongoClientURI key = split.getInputURI();
            if (CLIENTS_MAP.containsKey(key)) {
                lock = CLIENTS_MAP.get(key);
                LOG.info("*** retrieve lock for key: " + key + "; count=" + lock.getCount());
            } else {
                LOG.info("*** create lock for key: " + key);
                lock = new CountLock();
                CLIENTS_MAP.put(key, lock);
            }
        }
    }

    private class CountLock {
        private int count = 0;

        public void inc() {
            count++;
        }

        public void dec() {
            count--;
        }

        public int getCount() {
            return count;
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            lock.dec();
            LOG.info("*** close() called for tid: " + tid + "; lock now = " + lock.getCount());
            if (cursor != null) {
                cursor.close();
                // Only close the client after we're done with all the cursors.
                if (lock.getCount() <= 0) {
                    LOG.info("*** CLOSING CLIENT on tid: " + tid);
                    MongoConfigUtil.close(
                      cursor.getCollection().getDB().getMongo());
                }
            }
        }
    }

    @Override
    public Object getCurrentKey() {
        Object key = MongoPathRetriever.get(current, split.getKeyField());
        return null != key ? key : NullWritable.get();
    }

    @Override
    public BSONObject getCurrentValue() {
        return current;
    }

    @Override
    public float getProgress() {
        try {
            synchronized (lock) {
                return cursor.hasNext() ? 0.0f : 1.0f;
            }
        } catch (MongoException e) {
            return 1.0f;
        }
    }

    @Override
    public void initialize(
      final InputSplit split, final TaskAttemptContext context) {
        tid = context.getTaskAttemptID();
        // TODO: this change breaks old-style MRR
        synchronized (lock) {
            this.cursor = this.split.getCursor();
            lock.inc();
            LOG.info("*** initialize() called on tid: " + tid + "; count = " + lock.getCount());
        }
    }

    @Override
    public boolean nextKeyValue() {
        try {
            synchronized (lock) {
                // We assume the lock has a nonzero count here... why?
                // - because we assume we called initialize() previous to this,
                // - which increments the counter.
                try {
                    if (!cursor.hasNext()) {
                        LOG.info("Read " + seen + " documents from:");
                        LOG.info(split.toString());
                        return false;
                    }
                } catch (IllegalStateException ise) {
                    LOG.info("Caught ISE with count: " + lock.getCount()
                      + "; TID: " + tid.toString());
                    throw ise;
                }

                current = cursor.next();
                seen++;
            }

            return true;
        } catch (MongoException e) {
            LOG.error("Exception reading next key/val from mongo: " + e.getMessage());
            return false;
        }
    }
}
