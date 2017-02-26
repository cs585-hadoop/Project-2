package project2;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class P3DataGeneratorKmean {
	static void points(int num,String filename) throws IOException {
		Random rand = new Random();
		FileOutputStream fos = new FileOutputStream(new File(filename));
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		float col, row = 0;
		String buffer = null;
		for (int i = 0; i < num; i++) {
			col = rand.nextInt(10000) + rand.nextFloat();
			row = rand.nextInt(10000) + rand.nextFloat();
			buffer = col + "," + row;
			if (i != num - 1) {
				buffer = buffer + "\n";
			}
			bos.write(buffer.getBytes());
			bos.flush();
		}
		bos.close();
	}
	
	public static void main(String[] args) throws IOException {
		points(7000000,"points.txt");
		points(Integer.parseInt(args[0]),"seeds.txt");
	}

}
