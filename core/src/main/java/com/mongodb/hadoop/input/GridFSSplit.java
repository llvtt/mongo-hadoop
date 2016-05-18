package com.mongodb.hadoop.input;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.types.ObjectId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class GridFSSplit extends InputSplit
  implements org.apache.hadoop.mapred.InputSplit {

    private static final Log LOG = LogFactory.getLog(GridFSSplit.class);

    private ObjectId fileId;
    private int chunkSize;
    private long fileLength;
    private GridFS _gridfs;
    private int chunkId;
    private MongoClientURI inputURI;

    // Zero-args constructor required for mapred.InputSplit.
    public GridFSSplit() {}

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

    private GridFSDBFile getFile() {
        GridFSDBFile file = getGridFS().find(fileId);
        if (null == file) {
            LOG.warn("No file found for id: " + fileId + " in root "
                + "collection: " + inputURI);
        }
        return file;
    }

    /**
     * Get the value for a key in this file.
     * @param key the key to look up
     * @return the value for the key
     */
    public Object get(final String key) {
        return getFile().get(key);
    }

    /**
     * Get the metadata associated with this file.
     * @return the metadata as a {@code DBObject}.
     */
    public DBObject getMetadata() {
        return getFile().getMetaData();
    }

    /**
     * Read data from the GridFS chunk represented by this split.
     * @return The decoded data from the chunk
     * @throws IOException if there is an exception reading the data.
     */
    public String getData() throws IOException {
        InputStream fileStream = getFile().getInputStream();

        // Skip to chunk. GridFSInputStream will do what we want here.
        // noinspection ResultOfMethodCallIgnored
        fileStream.skip(chunkSize * chunkId);
        byte[] buff = new byte[chunkSize];
        int bytesRead = fileStream.read(buff, 0, chunkSize);
        return new String(buff, 0, bytesRead);
    }

    @Override
    public long getLength() throws IOException {
        return fileLength;
    }

    @Override
    public String[] getLocations() throws IOException {
        List<String> hosts = inputURI.getHosts();
        return hosts.toArray(new String[hosts.size()]);
    }

    // Implement mapred.InputSplit.
    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeUTF(inputURI.toString());
        out.write(fileId.toByteArray());
        out.writeInt(chunkId);
        out.writeLong(fileLength);
        out.writeInt(chunkSize);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        inputURI = new MongoClientURI(in.readUTF());
        byte[] oidBytes = new byte[12];
        in.readFully(oidBytes);
        fileId = new ObjectId(oidBytes);
        chunkId = in.readInt();
        fileLength = in.readLong();
        chunkSize = in.readInt();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("GridFSSplit{");
        if (inputURI != null) {
            sb.append("inputURI hosts=")
              .append(inputURI.getHosts())
              .append(", ");
        }
        if (fileId != null) {
            sb.append("fileId=").append(fileId).append(", ");
        }
        return sb
          .append("chunkSize=").append(chunkSize)
          .append(", fileLength=").append(fileLength)
          .append(", chunkId=").append(chunkId).append("}").toString();
    }

    @Override
    public int hashCode() {
        int hash = (null == inputURI ? 0 : inputURI.hashCode());
        hash = hash * 31 + (null == fileId ? 0 : fileId.hashCode());
        return hash * 31 + chunkId;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof GridFSSplit) {
            GridFSSplit other = (GridFSSplit) obj;
            return (null == inputURI
              ? null == other.inputURI : inputURI .equals(other.inputURI)) &&
              fileId.equals(other.fileId) &&
              chunkId == other.chunkId;
        }
        return false;
    }
}
