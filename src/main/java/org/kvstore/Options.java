package org.kvstore;

import java.util.HashMap;

import org.apache.log4j.Logger;

public class Options {
	private static final Logger log = Logger.getLogger(Options.class);
	private final HashMap<String, String> opts = new HashMap<String, String>();
	//
	// String
	public Options set(final String key, final String value) {
		opts.put(key, value);
		return this;
	}
	public String getString(final String key) {
		return getString(key, null);
	}
	public String getString(final String key, final String defaultValue) {
		final String value = opts.get(key);
		if ((value != null) && !value.isEmpty()) {
			return value;
		}
		return defaultValue;
	}
	//
	// Integer
	public Options set(final String key, final int value) {
		opts.put(key, String.valueOf(value));
		return this;
	}
	public int getInt(final String key) {
		return getInt(key, Integer.MIN_VALUE);
	}
	public int getInt(final String key, final int defaultValue) {
		try {
			final String value = opts.get(key);
			if (value != null) {
				return Integer.parseInt(value);
			}
		} catch (NumberFormatException e) {
			log.error("NumberFormatException in getInt("+key+")", e);
		}
		return defaultValue;
	}
	//
	// Boolean
	public Options set(final String key, final boolean value) {
		opts.put(key, String.valueOf(value));
		return this;
	}
	public boolean getBoolean(final String key) {
		return getBoolean(key, false);
	}
	public boolean getBoolean(final String key, final boolean defaultValue) {
		final String value = opts.get(key);
		if (value != null) {
			return Boolean.parseBoolean(value);
		}
		return defaultValue;
	}
	//
	// Global
	public boolean contains(final String key) {
		return opts.containsKey(key);
	}
	public Options remove(final String key) {
		opts.remove(key);
		return this;
	}
	public String toString() {
		return opts.toString();
	}
	//
}
