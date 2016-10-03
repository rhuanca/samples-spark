
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
import org.apache.spark.api.java.function.VoidFunction;

public class Example9 {
	private static Logger logger = Logger.getLogger(Example9.class);
	private static final String EN_WIKI_PATH = "/home/rhuanca/test/enwiki-20150602-pages-articles-multistream.xml";

	private static class Element implements Serializable {
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
				int pagesCount = 0;
				List<Element> list = new ArrayList<>();
				logger.info("Starting to read pages.");
				while (reader.hasNext() && pagesCount < max ) {
					if (reader.next() == XMLStreamReader.START_ELEMENT) {
						String elementName = reader.getLocalName();
						if ("page".equals(elementName)) {
							String title = "";
							String text = "";
							boolean readPage = true;
							while (reader.hasNext() && readPage && pagesCount < max ) {
								int eventType = reader.next();
								switch (eventType) {
									case XMLStreamReader.START_ELEMENT :
										elementName = reader.getLocalName();
										if (elementName.equals("title"))
											title = reader.getElementText();
										if (elementName.equals("text"))
											text = reader.getElementText();
									case XMLStreamReader.END_ELEMENT :
										if (flushCount >= 100) {
											q.put(list);
											list = new ArrayList<>();
											pagesCount += flushCount;
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
				logger.info("finished to read pages.");
				
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	private static class ProcessElement
			implements
				VoidFunction<Example9.Element>,
				Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void call(Element t) throws Exception {
			System.out.println(">>> t.title = " + t.title);

		}
	}
	private static class Consumer implements Runnable {

		private ArrayBlockingQueue<List<Element>> q;
		private SparkConf conf;
		private int maxPages;

		public Consumer(ArrayBlockingQueue<List<Element>> q, SparkConf conf, int maxPages) {
			this.conf = conf;
			this.q = q;
			this.maxPages = maxPages;
		}

		@Override
		public void run() {
			JavaSparkContext jssc = new JavaSparkContext(conf);
			int processed = 0;
			logger.info("starting to process pages.");
			while (!q.isEmpty() && processed <= maxPages ) {
				List<Element> list = q.poll();
				processed += list.size();
				JavaRDD<Element> rdd = jssc.parallelize(list);
				rdd.foreach(new ProcessElement());
			}
			logger.info("processing pages finished.");
		}
	}

	public static void main(String args[]) throws IOException {
		int maxPages = 1000;
		ArrayBlockingQueue<List<Element>> q = new ArrayBlockingQueue<>(30000);
		SparkConf sparkConf = new SparkConf().setAppName("WikiWordCount")
				.setMaster("local[2]");

		ExecutorService service = Executors.newFixedThreadPool(2);
		service.execute(
				new Producer(new FileInputStream(EN_WIKI_PATH), q, maxPages));
		service.execute(new Consumer(q, sparkConf, maxPages));
		service.shutdown();
		while (!service.isTerminated());
	}
}
