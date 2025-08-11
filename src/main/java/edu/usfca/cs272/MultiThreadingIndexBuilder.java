package edu.usfca.cs272;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import edu.usfca.cs272.utils.WorkQueue;

/**
 * Utility class to build a thread-safe inverted index using multithreading.
 * Uses batch processing for improved performance.
 *
 * @author Aarav
 */
public class MultiThreadingIndexBuilder {

	/**
	 * Processes the input path and checks if it's a directory or file path. Uses
	 * multithreading for building the inverted index.
	 *
	 * @param inputPath the file path to use
	 * @param index     the thread-safe index to populate
	 * @param queue     the work queue for multithreading
	 * @throws IOException if an IO error occurs
	 */
	public static void build(Path inputPath, ThreadSafeInvertedIndex index, WorkQueue queue) throws IOException {
		if (Files.isDirectory(inputPath)) {
			buildDirectories(inputPath, index, queue);
		} else if (InvertedIndexBuilder.isTextFile(inputPath)) {
			queue.execute(new IndexTask(inputPath, index));
		}
		queue.finish();
	}

	/**
	 * Processes a directory to find all text files and build the inverted index.
	 * 
	 * This creates task but does not wait for them to finish.
	 *
	 * @param input the directory path to search
	 * @param index the thread-safe index to populate
	 * @param queue the work queue for multithreading
	 * @throws IOException if an IO error occurs
	 */
	public static void buildDirectories(Path input, ThreadSafeInvertedIndex index, WorkQueue queue) throws IOException {
		try (var streamDirectory = Files.newDirectoryStream(input)) {
			for (Path currentFile : streamDirectory) {
				if (Files.isDirectory(currentFile)) {
					buildDirectories(currentFile, index, queue);
				} else if (InvertedIndexBuilder.isTextFile(currentFile)) {
					queue.execute(new IndexTask(currentFile, index));
				}
			}
		}
	}

	/**
	 * Runnable task that indexes a single file.
	 */
	private static class IndexTask implements Runnable {
		/** The file to index */
		private final Path file;
		/** The thread-safe index */
		private final ThreadSafeInvertedIndex index;

		/**
		 * Initializes the indexing task.
		 *
		 * @param file  the file to index
		 * @param index the thread-safe index to populate
		 */
		public IndexTask(Path file, ThreadSafeInvertedIndex index) {
			this.file = file;
			this.index = index;
		}

		@Override
		public void run() {
			try {
				InvertedIndex local = new InvertedIndex();
				InvertedIndexBuilder.buildFile(file, local);
				index.merge(local);
			} catch (IOException e) {
				System.err.println("Unable to process file: " + file);
				throw new UncheckedIOException(e);
			}
		}
	}

	/** Private constructor prevents instantiation */
	private MultiThreadingIndexBuilder() {
	}
}