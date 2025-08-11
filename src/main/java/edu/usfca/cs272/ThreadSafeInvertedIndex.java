package edu.usfca.cs272;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.usfca.cs272.utils.MultiReaderLock;

/**
 * A thread-safe extension of the InvertedIndex class.
 * 
 * @author Aarav
 */
public class ThreadSafeInvertedIndex extends InvertedIndex {
	/** The lock used to protect concurrent access to the index */
	private final MultiReaderLock lock;

	/**
	 * Initializes a thread-safe inverted index.
	 */
	public ThreadSafeInvertedIndex() {
		super();
		this.lock = new MultiReaderLock();
	}

	@Override
	public void addWord(String word, String input, int position) {
		lock.writeLock().lock();
		try {
			super.addWord(word, input, position);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void addWords(List<String> words, String input, int start) {
		lock.writeLock().lock();
		try {
			super.addWords(words, input, start);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void merge(InvertedIndex other) {
		lock.writeLock().lock();
		try {
			super.merge(other);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public Set<String> getWords() {
		lock.readLock().lock();
		try {
			return super.getWords();
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public Set<String> getInputs(String word) {
		lock.readLock().lock();
		try {
			return super.getInputs(word);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public Set<Integer> getPositions(String word, String input) {
		lock.readLock().lock();
		try {
			return super.getPositions(word, input);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public List<QueryResult> exactSearch(Set<String> words) {
		lock.readLock().lock();
		try {
			return super.exactSearch(words);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public List<QueryResult> partialSearch(Set<String> words) {
		lock.readLock().lock();
		try {
			return super.partialSearch(words);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public void toJson(Path path) throws IOException {
		lock.readLock().lock();
		try {
			super.toJson(path);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public Map<String, Integer> getCounts() {
		lock.readLock().lock();
		try {
			return super.getCounts();
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public boolean hasWord(String word) {
		lock.readLock().lock();
		try {
			return super.hasWord(word);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public boolean hasInput(String word, String input) {
		lock.readLock().lock();
		try {
			return super.hasInput(word, input);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public boolean hasPosition(String word, String input, Integer position) {
		lock.readLock().lock();
		try {
			return super.hasPosition(word, input, position);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public boolean hasCount(String input) {
		lock.readLock().lock();
		try {
			return super.hasCount(input);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public int numWords() {
		lock.readLock().lock();
		try {
			return super.numWords();
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public int numInputs(String word) {
		lock.readLock().lock();
		try {
			return super.numInputs(word);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public int numPositions(String word, String input) {
		lock.readLock().lock();
		try {
			return super.numPositions(word, input);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public int numCounts() {
		lock.readLock().lock();
		try {
			return super.numCounts();
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public int getCount(String input) {
		lock.readLock().lock();
		try {
			return super.getCount(input);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public String toString() {
		lock.readLock().lock();
		try {
			return super.toString();
		} finally {
			lock.readLock().unlock();
		}
	}
}