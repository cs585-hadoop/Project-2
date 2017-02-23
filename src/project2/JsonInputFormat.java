package project2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class JsonInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputsplit,
			TaskAttemptContext taskattemptcontext) throws IOException, InterruptedException {
		return new JsonRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		return codec == null;
	}
}

class JsonRecordReader extends RecordReader<LongWritable, Text> {
	private LineRecordReader line = new LineRecordReader();
	private LongWritable key;
	private Text value = new Text();

	@Override
	public void close() throws IOException {
		if (line != null) {
			line.close();
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return line.getProgress();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		line.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		int count = 0;
		boolean flag = false;
		value.clear();
		while (line.nextKeyValue()) {
			key = line.getCurrentKey();
			value.append(line.getCurrentValue().toString().getBytes(), 0, line.getCurrentValue().toString().length());
			char[] buffer = line.getCurrentValue().toString().toCharArray();
			for (int i = 0; i < buffer.length; i++) {
				if (buffer[i] == '{') {
					flag = false;
					count++;
				}
				if (buffer[i] == '}') {
					count--;
					flag = (count == 0) ? true : false;
				}
			}
			if (count == 0 && flag == true)
				return true;
		}
		return false;
	}
}
