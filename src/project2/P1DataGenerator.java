package project2;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class P1DataGenerator {
	static void points(int num) throws IOException {
		Random rand = new Random();
		FileOutputStream fos = new FileOutputStream(new File("p.txt"));
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

	static void rects(int num) throws IOException {
		Random rand = new Random();
		FileOutputStream fos = new FileOutputStream(new File("r.txt"));
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		float col, row, height, width = 0;
		String buffer = null;
		for (int i = 0; i < num; i++) {
			col = rand.nextInt(10000) + rand.nextFloat();
			row = rand.nextInt(10000) + rand.nextFloat();
			width = rand.nextInt(5) + 1;
			height = rand.nextInt(20) + 1;
			buffer = "r" + i + "," + col + "," + row + "," + width + "," + height;
			if (i != num - 1) {
				buffer = buffer + "\n";
			}
			bos.write(buffer.getBytes());
			// bos.flush();
		}
		bos.close();
	}

	public static void main(String[] args) throws IOException {
		points(6000000);
		rects(3000000);
	}
}
