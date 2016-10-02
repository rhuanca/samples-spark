
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Example9 {
	private static Logger logger = Logger.getLogger(Example9.class);
	private static final String EN_WIKI_PATH = "/home/rhuanca/test/enwiki-20150602-pages-articles-multistream.xml";
	private static final int MAX = 20;

	private static class Element implements Serializable{
		private static final long serialVersionUID = 1L;
		
		final public String title;
		final public String text;

		public Element(String title, String text) {
			this.title = title;
			this.text = text;
		}

		@Override
		public String toString() {
			return "Element [title=" + title + ", text=" + text + "]";
		}
	}

	private static class Producer implements Runnable {
		private InputStream in;
		private ArrayBlockingQueue<List<Element>> q;
		private int max;

		public Producer(InputStream in, ArrayBlockingQueue<List<Element>> q,
				int max) {
			this.q = q;
			this.max = max;
			this.in = in;
		}

		@Override
		public void run() {

			try {
				XMLStreamReader reader = XMLInputFactory.newInstance()
						.createXMLStreamReader(in);
				int flushCount = 0;
				List<Element> list = new ArrayList<>();
				while (reader.hasNext()) {
					if (reader.next() == XMLStreamReader.START_ELEMENT) {
						String elementName = reader.getLocalName();
						if ("page".equals(elementName)) {
							String title = "";
							String text = "";
							boolean readPage = true;
							while (reader.hasNext() && readPage) {
								int eventType = reader.next();
								switch (eventType) {
									case XMLStreamReader.START_ELEMENT :
										elementName = reader.getLocalName();
										if (elementName.equals("title"))
											title = reader.getElementText();
										if (elementName.equals("text"))
											text = reader.getElementText();
									case XMLStreamReader.END_ELEMENT :
										if(flushCount >= 100) {
											q.put(list);
											list = new ArrayList<>();
											flushCount = 0;
										} else {
											list.add(new Element(title, text));
											flushCount++;
										}
										readPage = false;
								}
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	private static class Consumer implements Runnable {
		private ArrayBlockingQueue<List<Element>> q;
		private SparkConf conf;

		public Consumer(ArrayBlockingQueue<List<Element>> q, SparkConf conf) {
			this.conf = conf;
			this.q = q;
		}

		@Override
		public void run() {
			JavaSparkContext jssc = new JavaSparkContext(conf);
			while(!q.isEmpty()) {
				List<Element> list = q.poll();
				System.out.println(">>> list.size() = " + list.size());
				JavaRDD<Element> rdd = jssc.parallelize(list);
				System.out.println(">>> rdd.count() = " + rdd.count() );
			}
		}
	}

	public static void main(String args[]) throws IOException {
		ArrayBlockingQueue<List<Element>> q = new ArrayBlockingQueue<>(20);
		SparkConf sparkConf = new SparkConf().setAppName("WikiWordCount")
				.setMaster("local[2]");

		ExecutorService service = Executors.newFixedThreadPool(2);
		service.execute(
				new Producer(new FileInputStream(EN_WIKI_PATH), q, MAX));
		service.execute(new Consumer(q, sparkConf));
		service.shutdown();
		while (!service.isTerminated());
	}
}
