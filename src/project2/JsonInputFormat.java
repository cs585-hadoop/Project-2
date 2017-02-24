package project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class JsonInputFormat extends FileInputFormat<LongWritable, Text> {

	private static final double SPLIT_SLOP = 1.1; // 10% slop

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputsplit,
			TaskAttemptContext taskattemptcontext) throws IOException, InterruptedException {
		return new JsonRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (FileStatus file : listStatus(job)) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job.getConfiguration());
			long length = file.getLen();
			minSize = length / 5;
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			if ((length != 0) && isSplitable(job, path)) {
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);
				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
					splits.add(
							new FileSplit(path, length - bytesRemaining, splitSize, blkLocations[blkIndex].getHosts()));
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts()));
				}
			} else if (length != 0) {
				splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}
		return splits;
	}

	@Override
	protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
		return minSize;
	}
}

class JsonRecordReader extends RecordReader<LongWritable, Text> {
	private LineRecordReader line = new LineRecordReader();
	private LongWritable key;
	private Text value = new Text();
	static int count = 0;
	static String splited = "";

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
		value.clear();
		Pattern pattern = Pattern.compile(".*\\}.*");
		Matcher matcher;
		while (line.nextKeyValue()) {
			key = line.getCurrentKey();
			if (splited.length() > 0) {
				value.append(splited.getBytes(), 0, splited.getBytes().length);
			}
			value.append(line.getCurrentValue().toString().getBytes(), 0, line.getCurrentValue().toString().length());
			matcher = pattern.matcher(line.getCurrentValue().toString());
			if (matcher.matches()) {
				splited = "";
				return true;
			}
		}
		splited = value.toString();
		return false;
	}
}
