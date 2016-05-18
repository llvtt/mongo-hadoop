package com.mongodb.hadoop.input;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class GridFSSplit extends InputSplit {

    private final ObjectId fileId;
    private final int chunkSize;
    private final long fileLength;
    private GridFS _gridfs;
    private final int chunkId;
    private final MongoClientURI inputURI;

    public GridFSSplit(
      final MongoClientURI inputURI,
      final ObjectId fileId,
      final int chunkSize,
      final long fileLength,
      final int chunkId) {
        this.inputURI = inputURI;
        this.fileId = fileId;
        this.chunkSize = chunkSize;
        this.fileLength = fileLength;
        this.chunkId = chunkId;
    }

    private GridFS getGridFS() {
        if (null == _gridfs) {
            DBCollection rootCollection =
              MongoConfigUtil.getCollection(inputURI);
            _gridfs = new GridFS(
              rootCollection.getDB(), rootCollection.getName());
        }
        return _gridfs;
    }

    public String getData() throws IOException {
        GridFS gridFS = getGridFS();
        GridFSDBFile retrieved = gridFS.find(fileId);
        InputStream fileStream = retrieved.getInputStream();

        // Skip to chunk. GridFSInputStream will do what we want here.
        // noinspection ResultOfMethodCallIgnored
        fileStream.skip(chunkSize * chunkId);
        byte[] buff = new byte[chunkSize];
        int bytesRead = fileStream.read(buff, 0, chunkSize);
        return new String(buff, 0, bytesRead);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return fileLength;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        List<String> hosts = inputURI.getHosts();
        return hosts.toArray(new String[hosts.size()]);
    }
}
