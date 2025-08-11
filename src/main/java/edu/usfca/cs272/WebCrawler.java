package edu.usfca.cs272;

import static opennlp.tools.stemmer.snowball.SnowballStemmer.ALGORITHM.ENGLISH;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.usfca.cs272.utils.HtmlCleaner;
import edu.usfca.cs272.utils.HtmlFetcher;
import edu.usfca.cs272.utils.LinkFinder;
import edu.usfca.cs272.utils.WorkQueue;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

/**
 * A web crawler that fetches and indexes a single HTML page into a thread-safe
 * inverted index. Follows redirects and uses FileStemmer logic for text
 * processing.
 *
 * @author Aarav
 */
public class WebCrawler {
	/** The thread-safe inverted index */
	private final ThreadSafeInvertedIndex index;

	/** The work queue to use */
	private final WorkQueue queue;

	/** Tracks visited URIs to avoid reprocessing */
	private final Set<String> visited;

	/** total numbers of URLs to crawl */
	private final int total;

	/** Counter for crawled pages */
	private int crawledCount;

	/** Lock object for synchronization */
	private final Object lock;

	/**
	 * Creates a new web crawler with the specified index and work queue.
	 *
	 * @param index the thread-safe inverted index
	 * @param queue the work queue for multithreading
	 * @param total the total number of pages to crawl
	 */
	public WebCrawler(ThreadSafeInvertedIndex index, WorkQueue queue, int total) {
		this.index = index;
		this.queue = queue;
		this.visited = new HashSet<>();
		this.total = total;
		this.crawledCount = 0;
		this.lock = new Object();
	}

	/**
	 * Crawls the specified seed URI.
	 *
	 * @param seed the seed URI to crawl
	 */
	public void crawl(String seed) {
		try {
			URI uri = new URI(seed);
			String normalized = uri.toString().split("#", 2)[0];

			synchronized (visited) {
				if (visited.add(normalized)) {
					queue.execute(new CrawlerTask(uri, normalized));
				}
			}
		} catch (URISyntaxException e) {
			System.err.println("Invalid seed URI: " + seed);
		}
	}

	/**
	 * Runnable task to download, clean, stem, and index a page.
	 */
	private class CrawlerTask implements Runnable {
		/** The URI to fetch HTML content from. */
		private final URI uri;

		/** The original URI. */
		private final String original;

		/**
		 * Initializes the CrawlerTask
		 *
		 * @param uri      the URI to fetch HTML content from
		 * @param original the original URI
		 */
		public CrawlerTask(URI uri, String original) {
			this.uri = uri;
			this.original = original;
		}

		@Override
		public void run() {
			// Check if we've reached the limit
			synchronized (lock) {
				if (crawledCount >= total) {
					return;
				}
				crawledCount++;
			}

			// Fetch HTML content
			String html = HtmlFetcher.fetch(uri, 3);
			if (html == null) {
				return;
			}

			// Process content and links in a single task
			processPage(html);
		}

		/**
		 * Processes both the content and links of a page in a single task.
		 *
		 * @param html the html to process
		 */
		private void processPage(String html) {
			// Clean HTML once and reuse
			String forLinks = HtmlCleaner.stripBlockElements(html);
			String cleaned = HtmlCleaner.stripEntities(HtmlCleaner.stripTags(forLinks));
			
			// Process content first
			String[] words = FileStemmer.parse(cleaned);
			Stemmer stemmer = new SnowballStemmer(ENGLISH);
			InvertedIndex local = new InvertedIndex();
			int position = 1;
			for (String word : words) {
				String stem = stemmer.stem(word).toString();
				if (!stem.isEmpty()) {
					local.addWord(stem, original, position++);
				}
			}
			index.merge(local);

			// Process links
			List<URI> links = LinkFinder.listUris(uri, forLinks);
			List<String> newLinks = new ArrayList<>();

			// Minimize time spent in synchronized block
			synchronized (visited) {
				for (URI link : links) {
					if (visited.size() >= total) {
						break;
					}
					String cleanLink = link.toString().split("#", 2)[0];
					if (visited.add(cleanLink)) {
						newLinks.add(cleanLink);
					}
				}
			}

			// Queue new tasks outside of synchronized block
			for (String cleanLink : newLinks) {
				try {
					queue.execute(new CrawlerTask(new URI(cleanLink), cleanLink));
				} catch (URISyntaxException e) {
					// Skip invalid URIs
				}
			}
		}
	}
}