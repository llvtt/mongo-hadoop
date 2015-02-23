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

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;


public class BSONFileRecordReader implements  RecordReader<NullWritable, BSONWritable> {
    private final com.mongodb.hadoop.input.BSONFileRecordReader delegate =
            new com.mongodb.hadoop.input.BSONFileRecordReader();

    /**
     * Shim class to wrap a Configuration in a TaskAttemptContext for
     * compatibility with the Hadoop 2.0 interface. Instances of this class
     * have a fake TaskAttemptID.
     *
     * The new-style BSONFileRecordReader can check if its TaskAttemptContext
     * is a TaskAttemptContextShim to determine if the context actually has
     * valid data besides the wrapped Configuration.
     */
    public class TaskAttemptContextShim extends TaskAttemptContextImpl {
        public TaskAttemptContextShim(final Configuration conf) {
            super(conf, new TaskAttemptID());
        }
    }

    public void initialize(final InputSplit inputSplit, final Configuration conf) throws IOException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        org.apache.hadoop.mapreduce.lib.input.FileSplit newInputSplit =
                new org.apache.hadoop.mapreduce.lib.input.FileSplit(
                        fileSplit.getPath(),
                        fileSplit.getStart(),
                        fileSplit.getLength(),
                        fileSplit.getLocations()
                );
        delegate.initialize(newInputSplit, new TaskAttemptContextShim(conf));
    }

    public long getPos() throws IOException {
        return delegate.getPos();
    }

    @Override
    public boolean next(final NullWritable key, final BSONWritable value) throws IOException {
        if (delegate.nextKeyValue()) {
            value.setDoc(delegate.getCurrentValue());
            return true;
        }
        return false;
    }

    public float getProgress() throws IOException {
        return delegate.getProgress();
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public BSONWritable createValue() {
        return new BSONWritable();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

}
