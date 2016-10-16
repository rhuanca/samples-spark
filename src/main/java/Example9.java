
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

public class Example9 {
    private static Logger logger = Logger.getLogger(Example9.class);
    private static final String EN_WIKI_PATH =
            "/home/rhuanca/test/enwiki-20150602-pages-articles-multistream.xml";

    private static final class Print implements VoidFunction<JavaRDD<Element>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void call(JavaRDD<Element> elements) throws Exception {
        	elements.foreach(new VoidFunction<Example9.Element>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void call(Element t) throws Exception {
					logger.info("title: " + t.title);
					logger.info("text: " + t.text);
				}
			});
        }
    }

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

    private final static class CustomReceiver extends Receiver<Element> {
        private static final long serialVersionUID = 1L;

        private String path;

        private CustomReceiver(String path) {
            super(StorageLevel.MEMORY_AND_DISK_2());
            this.path = path;
        }

        public void receive() {
            try {
                XMLStreamReader reader = XMLInputFactory.newInstance()
                        .createXMLStreamReader(new FileInputStream(path));
                logger.info("Starting to read pages.");
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
                                    case XMLStreamReader.START_ELEMENT:
                                        elementName = reader.getLocalName();
                                        if (elementName.equals("title"))
                                            title = reader.getElementText();
                                        if (elementName.equals("text"))
                                            text = reader.getElementText();
                                    case XMLStreamReader.END_ELEMENT:
                                        // logger.info(">>>> title = " + title);
                                        // logger.info(">>>> text = " + text);
                                        store(new Element(title, text));
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

        @Override
        public void onStop() {
            // ignore silently
        }

        @Override
        public void onStart() {
            logger.info("Starting receiver...");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    receive();
                }
            }).start();
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        SparkConf conf = new SparkConf().setAppName("WikiWordCount").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));

        CustomReceiver receiver = new CustomReceiver(EN_WIKI_PATH);

        JavaReceiverInputDStream<Element> s = jsc.receiverStream(receiver);
        s.foreachRDD(new Print());
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
