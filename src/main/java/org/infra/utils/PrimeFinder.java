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

import java.util.Arrays;

public final class PrimeFinder {
	/**
	 * The prime number list 1-Integer.MAX_VALUE.
	 */
	private static final int[] primeCapacities = { 
		1, 2, 7, 17, 37, 79,
		149, 271, 479, 827, 1399, 2309, 3739, 5981, 9391, 14557, 22291,
		33757, 50627, 75181, 110647, 161507, 233939, 336397, 480409,
		681647, 961273, 1347733, 1879151, 2606353, 3596851, 4940051,
		6753841, 9193181, 12461041, 16822489, 22622599, 30309211,
		40461959, 53828963, 71373193, 94330987, 124285687, 163260059,
		213832237, 279280621, 363764831, 472551293, 612293659,
		791381131, 1020370069, 1312518343, 1684445207, 2147483647
	};

	/**
	 * Returns a prime number which is very close to
	 * <code>desiredCapacity</code>
	 * 
	 * @param desiredCapacity the capacity desired by the user.
	 * @return the capacity which should be used for a hash.
	 */
	public static final int nextPrime(final int desiredCapacity) {
		final int i = Arrays.binarySearch(primeCapacities, desiredCapacity);
		return primeCapacities[((i < 0) ? ((-i) - 1) : i)];
	}

}

