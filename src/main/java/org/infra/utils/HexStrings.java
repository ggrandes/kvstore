/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.infra.utils;

/**
 * Hex Strings 
 * This class is Thread-Safe
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class HexStrings {
	/**
	 * Basic table of hex characters
	 */
	private static final char[] HEX_TABLE = "0123456789abcdef".toCharArray();

	/**
	 * Return hex string (zero-left-padded) of an native byte
	 * @param input the number
	 * @return string
	 */
	public static final String nativeAsHex(final byte input) {
		return nativeAsHex(input, 8);
	}

	/**
	 * Return hex string (zero-left-padded) of an native short
	 * @param input the number
	 * @return string
	 */
	public static final String nativeAsHex(final short input) {
		return nativeAsHex(input, 16);
	}

	/**
	 * Return hex string (zero-left-padded) of an native int
	 * @param input the number
	 * @return string
	 */
	public static final String nativeAsHex(final int input) {
		return nativeAsHex(input, 32);
	}

	/**
	 * Return hex string (zero-left-padded) of an native long
	 * @param input the number
	 * @return string
	 */
	public static final String nativeAsHex(final long input) {
		return nativeAsHex(input, 64);
	}

	/**
	 * Return hex string (zero-left-padded) of an native number
	 * @param input the number
	 * @param bits the size in bits for a number (64 for long, 32 for int, 16 for short, 8 for byte)
	 * @return string
	 */
	public static final String nativeAsHex(final long input, final int bits) {
		final char[] sb = new char[(bits > 64 ? 64 : (bits < 8 ? 8 : bits)) >> 2];
		final int len = (sb.length - 1);

		for (int i = 0; i <= len; i++) { // MSB
			sb[i] = HEX_TABLE[((int) (input >>> ((len - i) << 2))) & 0xF];
		}
		return new String(sb);
	}

	/**
	 * Return hexdump of byte-array
	 * @param buf is byte array to dump
	 * @param limit size to dump from 0-offset to limit
	 * @return String representation of hexdump
	 */
	public final static String byteArrayAsHex(final byte[] buf, final int limit) {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < limit; ++i ) {
			if ((i % 16) == 0) { // print offset
				sb.append(nativeAsHex(i, 32)).append("  ");
			}
			else if (((i) % 8) == 0) { // split on qword
				sb.append(" ");
			}
			sb.append(nativeAsHex((buf[i] & 0xFF), 8)).append(" "); // hex byte
			if (((i % 16) == 15) || (i == (buf.length-1))) {
				for (int j = (16 - (i % 16)); j > 1; j--) { // padding non exist bytes
					sb.append("   ");
				}
				sb.append(" |"); // byte columns
				final int start = ((i/16) * 16);
				final int end = ((buf.length < i + 1) ? buf.length : (i+1));
				for (int j = start; j < end; ++j) {
					if ((buf[j] >= 32) && (buf[j] <= 126)) {
						sb.append((char)buf[j]);
					}
					else {
						sb.append("."); // non-printable character
					}
				}
				sb.append("|\n"); // end column
			}
		}
		return sb.toString();
	}

	public static void main(String[] args) {
		final byte[] buf = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Duis posuere massa vitae urna ultricies vitae tempor magna mollis.".getBytes();
		System.out.println(byteArrayAsHex(buf, buf.length));
	}
}
