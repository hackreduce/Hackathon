/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hackreduce.mappers;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class XMLRecordReader extends RecordReader<Text, Text>{

    private static final Logger LOG = Logger.getLogger(XMLRecordReader.class.getName());
    
    public static final String BEGIN = "hadoop.mapred.xmlrecordreader.begin";
    public static final String END = "hadoop.mapred.xmlrecordreader.end";
    
    private String _beginMark;
    private String _endMark;
    private CompressionCodecFactory _compressionCodecs = null;
    private long _start;
    private long _pos;
    private long _end;
    private long _recordCount;
    private long _nextStatusRecords = 1;
    private long _nextCalledCount;
    private int _statusMaxRecordChars;
    private FileSplit _split;
    private FSDataInputStream _fileInputStream;
    private BufferedInputStream _bufferedInputStream; // Wrap FSDataInputStream for efficient backward seeks 
    private Text _key = null;
    private Text _value = null;
    private Text _fileName = null;


    /**
     * Define the start and end tags that are considered to contain a record for Hadoop.
     *
     * @param job
     * @param startTag XML tag that defines the start of a record - e.g. &lt;record&gt;
     * @param endTag XML tag that defines the end of a record - e.g. &lt;/record&gt;
     */
    public static void setRecordTags(Job job, String startTag, String endTag) {
    	job.getConfiguration().set(BEGIN, startTag);
        job.getConfiguration().set(END, endTag);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        _beginMark = conf.get(BEGIN);
        _endMark = conf.get(END);
        
        _split = (FileSplit)split;
        _start = _split.getStart();
        _end = _start + _split.getLength();
        
        _pos = _start;
        
        final Path file = _split.getPath();
        _fileName = new Text(file.getName());
        _compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = _compressionCodecs.getCodec(file);
        
        LOG.info("XMLRecordReader.initialize: " + " start=" + _start + " end=" + _end);
        
        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(conf);
        _fileInputStream = fs.open(file);
        _fileInputStream.seek(_start);
        if (codec != null) {
          _bufferedInputStream = new BufferedInputStream(codec.createInputStream(_fileInputStream));
        } else {
          _bufferedInputStream = new BufferedInputStream(_fileInputStream);
        }
        seekNextRecordBoundary();
    }
    
    @Override
    public void close() throws IOException {
        if (_bufferedInputStream != null) {
            _bufferedInputStream.close();
          }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return _key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return _value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (_start == _end) {
            return 0.0f;
          } else {
            return Math.min(1.0f, (_pos - _start) / (float)(_end - _start));
          }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        _nextCalledCount++;
        if (_pos >= _end) {
          return false;
        }

        DataOutputBuffer buf = new DataOutputBuffer();
        if (!readUntilMatchBegin()) {
          return false;
        }
        if (!readUntilMatchEnd(buf)) {
          return false;
        }

        // There is only one elem..key/value splitting is not done here.
        byte[] record = new byte[buf.getLength()];
        System.arraycopy(buf.getData(), 0, record, 0, record.length);

        numRecStats(record, 0, record.length);

        _key = _fileName;
        _value = new Text(record);

        return true;
    }
    
    private void numRecStats(byte[] record, int start, int len) throws IOException {
        _recordCount++;
        if (_recordCount == _nextStatusRecords) {
          String recordStr = new String(record, start, Math.min(len, _statusMaxRecordChars), "UTF-8");
          _nextStatusRecords += 100;//*= 10;
          String status = getStatus(recordStr);
          LOG.info(status);
        }
    }
    
    private String getStatus(CharSequence record) {
        long pos = -1;
        try {
          pos = _fileInputStream.getPos();
        } catch (IOException io) {
        }
        String recStr;
        if (record.length() > _statusMaxRecordChars) {
          recStr = record.subSequence(0, _statusMaxRecordChars) + "...";
        } else {
          recStr = record.toString();
        }
        String unqualSplit = _split.getPath().getName() + ":" + _split.getStart() + "+" + _split.getLength();
        String status = "HSTR + " + _recordCount + ". pos=" + pos + " " + unqualSplit + " Processing record=" + recStr;
        status += " " + _split.getPath().getName();
        return status;
      }

    
    private void seekNextRecordBoundary() throws IOException {
        readUntilMatchBegin();
    }

    boolean readUntilMatchBegin() throws IOException {
        return fastReadUntilMatch(_beginMark, false, null);
    }

    private boolean readUntilMatchEnd(DataOutputBuffer buf) throws IOException {
      return fastReadUntilMatch(_endMark, true, buf);
    }

    boolean fastReadUntilMatch(String textPat, boolean includePat, DataOutputBuffer outBufOrNull) throws IOException {
      byte[] cpat = textPat.getBytes("UTF-8");
      int m = 0;
      boolean match = false;
      int msup = cpat.length;
      int LL = 120000 * 10;

      _bufferedInputStream.mark(LL); // large number to invalidate mark
      while (true) {
        int b = _bufferedInputStream.read();
        if (b == -1) break;

        byte c = (byte) b; // this assumes eight-bit matching. OK with UTF-8
        if (c == cpat[m]) {
          m++;
          if (m == msup) {
            match = true;
            break;
          }
        } else {
          _bufferedInputStream.mark(LL); // rest mark so we could jump back if we found a match
          if (outBufOrNull != null) {
            outBufOrNull.write(cpat, 0, m);
            outBufOrNull.write(c);
            _pos += m + 1;
          } else {
            _pos += m + 1;
            if (_pos >= _end) {
              break;
            }
          }
          m = 0;
        }
      }
      if (!includePat && match) {
        _bufferedInputStream.reset();
      } else if (outBufOrNull != null) {
        outBufOrNull.write(cpat);
        _pos += msup;
      }
      return match;
    }

}
