package edu.usfca.cs272;

import static opennlp.tools.stemmer.snowball.SnowballStemmer.ALGORITHM.ENGLISH;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

/**
 * Class responsible to total word counts and word Inverted Indexes across
 * multiple files, using the processFile and processDirectories methods.
 * 
 * @author Aarav Gupta
 */
public class InvertedIndexBuilder {

	/**
	 * Process the inputPath and checks if its a directory or a file path.
	 *
	 * @param inputPath the file path to use
	 * @param index     stores the InvertedIndexes
	 * @throws IOException if an IO error occurs
	 */
	public static void buildPath(Path inputPath, InvertedIndex index) throws IOException {
		if (Files.isDirectory(inputPath)) {
			InvertedIndexBuilder.buildDirectories(inputPath, index);
		} else {
			InvertedIndexBuilder.buildFile(inputPath, index);
		}
	}

	/**
	 * Process the file to get the values for wordCounts and Inverted Indexes
	 *
	 * @param inputPath the file path to use
	 * @param index     stores the InvertedIndexes
	 * @throws IOException if an IO error occurs
	 * 
	 * @see Files#newBufferedReader(Path, java.nio.charset.Charset)
	 * @see StandardCharsets#UTF_8
	 */
	public static void buildFile(Path inputPath, InvertedIndex index) throws IOException {
		try (BufferedReader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8)) {
			String line;
			int position = 1;
			Stemmer stemmer = new SnowballStemmer(ENGLISH);
			// Not recalling inputPath.toString multiple times
			String location = inputPath.toString();
			while ((line = reader.readLine()) != null) {
				String[] words = FileStemmer.parse(line);
				for (String word : words) {
					index.addWord(stemmer.stem(word).toString(), location, position);
					++position;
				}
			}
		}
	}

	/**
	 * Process the directory.
	 *
	 * @param inputPath the file path to use
	 * @param index     stores the InvertedIndexes
	 * @throws IOException if an IO error occurs
	 * @see #buildFile(Path inputPath, InvertedIndex index)
	 */
	public static void buildDirectories(Path inputPath, InvertedIndex index) throws IOException {
		try (var streamDirectory = Files.newDirectoryStream(inputPath)) {
			for (Path currentFile : streamDirectory) {
				if (Files.isDirectory(currentFile)) {
					buildDirectories(currentFile, index);
				} else if (isTextFile(currentFile)) {
					buildFile(currentFile, index);
				}
			}
		}
	}

	/**
	 * Checks if a file is a text file
	 *
	 * @param path the file path to check
	 * @return returns true if the file is a text file and false if it is not
	 */
	public static boolean isTextFile(Path path) {
		String fileName = path.getFileName().toString().toLowerCase();
		return fileName.endsWith(".txt") || fileName.endsWith(".text");
	}

	/** Prevent instantiating this class of static methods. */
	private InvertedIndexBuilder() {
	}
}
