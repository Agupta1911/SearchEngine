package edu.usfca.cs272;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Class responsible to create an inverted index structure storing the total
 * word counts and word Inverted Indexes across multiple files.
 * 
 * @author Aarav Gupta
 */
public class InvertedIndex {
	/**
	 * counts stores the count of words
	 */
	private final TreeMap<String, Integer> counts;
	/**
	 * index stores the Inverted Indexes of the words
	 */
	private final TreeMap<String, TreeMap<String, TreeSet<Integer>>> index;

	/**
	 * Constructor for the data structure class that initializes an empty inverted
	 * index.
	 */
	public InvertedIndex() {
		this.counts = new TreeMap<>();
		this.index = new TreeMap<>();
	}

	/**
	 * Adds all words from a list to the inverted index for a specific file.
	 *
	 * @param words the list of words to add
	 * @param input the file where the words appear
	 * @param start the starting position of the first word
	 */
	public void addWords(List<String> words, String input, int start) {
		int position = start;
		for (String word : words) {
			addWord(word, input, position++);
		}
	}

	/**
	 * Adds the Inverted Index of each word in the file
	 *
	 * @param word     the word to add
	 * @param input    the file where the word appears
	 * @param position the position of the word in the file
	 */
	public void addWord(String word, String input, int position) {
		TreeMap<String, TreeSet<Integer>> inputs = index.get(word);
		if (inputs == null) {
			inputs = new TreeMap<>();
			index.put(word, inputs);
		}

		TreeSet<Integer> positions = inputs.get(input);
		if (positions == null) {
			positions = new TreeSet<>();
			inputs.put(input, positions);
		}
		if (positions.add(position)) {
			counts.put(input, counts.getOrDefault(input, 0) + 1);
		}
	}

	/**
	 * Adds all entries from another inverted index to this index. Useful for
	 * combining local indexes into the main index. IMPORTANT: This can only be
	 * called when the locations don't overlap
	 *
	 * @param other the other inverted index to merge from
	 */
	public void merge(InvertedIndex other) {
		for (var otherEntry : other.index.entrySet()) {
			String otherWord = otherEntry.getKey();
			var otherInputs = otherEntry.getValue();

			var thisInputs = this.index.get(otherWord);

			if (thisInputs == null) {
				this.index.put(otherWord, otherInputs);
			} else {
				for (var inputEntry : otherInputs.entrySet()) {
					String input = inputEntry.getKey();
					var otherPositions = inputEntry.getValue();

					var thisPositions = thisInputs.get(input);
					if (thisPositions == null) {
						thisInputs.put(input, otherPositions);
					} else {
						thisPositions.addAll(otherPositions);
					}
				}
			}
		}

		for (var otherEntry : other.counts.entrySet()) {
			String input = otherEntry.getKey();
			int count = otherEntry.getValue();
			this.counts.put(input, this.counts.getOrDefault(input, 0) + count);
		}
	}

	/**
	 * Checks if a word exists in the index.
	 *
	 * @param word the word to check
	 * @return index.containsKey(word) true if the word exists, false otherwise
	 */
	public boolean hasWord(String word) {
		return index.containsKey(word);
	}

	/**
	 * Checks if the specific file contains a given word.
	 *
	 * @param word  the word to check
	 * @param input the file to check
	 * @return true if the word exists in the file, false otherwise
	 */
	public boolean hasInput(String word, String input) {
		TreeMap<String, TreeSet<Integer>> inputs = index.get(word);
		return inputs != null && inputs.containsKey(input);
	}

	/**
	 * Checks if a word exists at a specific position in a file.
	 *
	 * @param word     the word to check
	 * @param input    the file to check
	 * @param position the position to check
	 * @return true if the word exists at the position, false otherwise
	 */
	public boolean hasPosition(String word, String input, Integer position) {
		TreeMap<String, TreeSet<Integer>> inputs = index.get(word);
		if (inputs == null) {
			return false;
		}
		TreeSet<Integer> positions = inputs.get(input);
		return positions != null && positions.contains(position);
	}

	/**
	 * Checks if the word count exists for a specific file.
	 *
	 * @param input the file to check
	 * @return true if the file has a word count, false otherwise
	 */
	public boolean hasCount(String input) {
		return counts.containsKey(input);
	}

	/**
	 * Gets the number of unique words in the index.
	 *
	 * @return the number of unique words
	 */
	public int numWords() {
		return index.size();
	}

	/**
	 * Gets the number of files that contain a specific word.
	 *
	 * @param word the word to check
	 * @return the number of files containing the word
	 */
	public int numInputs(String word) {
		TreeMap<String, TreeSet<Integer>> inputs = index.get(word);
		return inputs == null ? 0 : inputs.size();
	}

	/**
	 * Gets the number of occurrences of a word in a specific file.
	 *
	 * @param word  the word to check
	 * @param input the file to check
	 * @return the number of occurrences of the word in the file
	 */
	public int numPositions(String word, String input) {
		TreeMap<String, TreeSet<Integer>> inputs = index.get(word);
		if (inputs == null) {
			return 0;
		}
		TreeSet<Integer> positions = inputs.get(input);
		return positions == null ? 0 : positions.size();
	}

	/**
	 * Gets the number of locations of files that have counts.
	 *
	 * @return the number of locations of files that have counts.
	 */
	public int numCounts() {
		return counts.size();
	}

	/**
	 * Gets the word count for a specific file.
	 *
	 * @param input the file to get the count for
	 * @return the word count, or 0 if the file doesn't exist
	 */
	public int getCount(String input) {
		return counts.getOrDefault(input, 0);
	}

	/**
	 * Gets the unmodifiable count of words.
	 *
	 * @return counts The unmodifiable Word Count Mapping
	 */
	public Map<String, Integer> getCounts() {
		return Collections.unmodifiableMap(counts);
	}

	/**
	 * Gets the unmodifiable set of file names which have a specific word.
	 *
	 * @param word the word to get the index for
	 * @return the unmodifiable set of file names which have the word, or empty set
	 *         if the word doesn't exist
	 */
	public Set<String> getInputs(String word) {
		Map<String, TreeSet<Integer>> inputs = index.get(word);
		return inputs == null ? Collections.emptySet() : Collections.unmodifiableSet(inputs.keySet());
	}

	/**
	 * Gets the unmodifiable Set of positions of a word in a specific file.
	 *
	 * @param word  the word to check
	 * @param input the file to check
	 * @return the unmodifiable set of positions, or empty set if the file or word
	 *         doesn't exist
	 */
	public Set<Integer> getPositions(String word, String input) {
		Map<String, TreeSet<Integer>> inputs = index.get(word);
		if (inputs == null) {
			return Collections.emptySet();
		}
		TreeSet<Integer> positions = inputs.get(input);
		return positions == null ? Collections.emptySet() : Collections.unmodifiableSet(positions);
	}

	/**
	 * Returns the unmodifiable set of words in the inverted index.
	 * 
	 * @return the unmodifiable set of words
	 */
	public Set<String> getWords() {
		return Collections.unmodifiableSet(index.keySet());
	}

	/**
	 * Calls the method to write the Inverted Indexes to a specified file path
	 *
	 * @param path the path of the file to write to.
	 * @throws IOException if an IO error occurs
	 */
	public void toJson(Path path) throws IOException {
		JsonWriter.writeInvertedIndexes(index, path);
	}

	/**
	 * Returns a string representation of the inverted index.
	 *
	 * @return the string representation
	 */
	@Override
	public String toString() {
		return index.toString();
	}

	/**
	 * Class to represent a single search result and implements Comparable
	 * Interface. It is also used for ranking search results.
	 */
	public class QueryResult implements Comparable<QueryResult> {
		/** The file to use where the matches were found. */
		private final String location;

		/** The number of matches found in the file. */
		private int matches;

		/** The score which will be calculated. */
		private double score;

		/**
		 * Initializes all the variables needed for the search result.
		 *
		 * @param file the file path where the match was found
		 */
		public QueryResult(String file) {
			this.location = file;
			this.matches = 0;
			this.score = 0;
		}

		/**
		 * Returns the file.
		 *
		 * @return file the file path as a string
		 */
		public String getFile() {
			return location;
		}

		/**
		 * Returns the number of matches found in the file.
		 *
		 * @return matches the match count.
		 */
		public int getMatches() {
			return matches;
		}

		/**
		 * Returns the score.
		 *
		 * @return score the calculated score
		 */
		public double getScore() {
			return score;
		}

		/**
		 * Updates the matches and recalculates the score.
		 *
		 * @param matches new match count
		 */
		private void addMatches(int matches) {
			this.matches += matches;
			this.score = (double) this.matches / counts.get(location);
		}

		/**
		 * Compares this search result to another to sort them, the results are ordered
		 * by 3 factors, each with descending priority.: Score, higher scores are ranked
		 * first, this is the first measure If the scores are the same, then Match count
		 * is used, higher number of matches are ranked first, this is the second
		 * measure, If then the score and matches are the same, File path is used to
		 * rank them.
		 * 
		 * @param other the search result to compare against
		 * @return a negative integer, zero, or positive integer as this result
		 */
		@Override
		public int compareTo(QueryResult other) {
			int scoreCompare = Double.compare(other.score, this.score);
			if (scoreCompare != 0) {
				return scoreCompare;
			}
			int matchCompare = Integer.compare(other.matches, this.matches);
			if (matchCompare != 0) {
				return matchCompare;
			}
			return this.location.compareToIgnoreCase(other.location);
		}
	}

	/**
	 * Search method that decides whether to perform an exact or partial search
	 *
	 * @param words   the set of cleaned, stemmed, and sorted query words
	 * @param partial boolean value whether partial or exact search should occur
	 * @return return either partialSearch or exactSearch
	 */
	public List<QueryResult> search(Set<String> words, boolean partial) {
		return partial ? partialSearch(words) : exactSearch(words);
	}

	/**
	 * Exact search method that takes pre-parsed words and returns sorted search
	 * results.
	 *
	 * @param words the set of cleaned, stemmed, and sorted query words
	 * @return a sorted list of QuerySearch results
	 */
	public List<QueryResult> exactSearch(Set<String> words) {
		Map<String, QueryResult> matchLookup = new HashMap<>();
		List<QueryResult> results = new ArrayList<>();

		for (String word : words) {
			TreeMap<String, TreeSet<Integer>> locations = index.get(word);
			if (locations != null) {
				processMatches(locations, matchLookup, results);
			}
		}
		Collections.sort(results);
		return results;
	}

	/**
	 * Partial search method that finds words starting with pre-parsed words.
	 *
	 * @param words the set of cleaned, stemmed, and sorted query words
	 * @return a sorted list of Partial search results
	 */
	public List<QueryResult> partialSearch(Set<String> words) {
		Map<String, QueryResult> matchLookup = new HashMap<>();
		List<QueryResult> results = new ArrayList<>();

		for (String word : words) {
			/*
			 * tailMap makes code more efficient as only search through words greater than
			 * or equal to the word we are doing a search for
			 * https://www.geeksforgeeks.org/treemap-tailmap-method-in-java/
			 */
			for (Map.Entry<String, TreeMap<String, TreeSet<Integer>>> entry : index.tailMap(word).entrySet()) {
				String indexWord = entry.getKey();
				if (!indexWord.startsWith(word)) {
					break;
				}

				processMatches(entry.getValue(), matchLookup, results);
			}
		}

		Collections.sort(results);
		return results;
	}

	/**
	 * The helper method to process and update query matches for a given set of
	 * entries.
	 *
	 * @param locations   the map of file names to positions
	 * @param matchLookup the map to track existing QueryResult entries
	 * @param results     the list to collect final sorted results
	 */
	private void processMatches(Map<String, TreeSet<Integer>> locations, Map<String, QueryResult> matchLookup,
			List<QueryResult> results) {
		for (Map.Entry<String, TreeSet<Integer>> entry : locations.entrySet()) {
			String location = entry.getKey();
			int count = entry.getValue().size();

			QueryResult current = matchLookup.get(location);
			if (current == null) {
				current = new QueryResult(location);
				matchLookup.put(location, current);
				results.add(current);
			}
			current.addMatches(count);
		}
	}
}