package com.gslab.pepper.input;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.InputStream;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The FieldDataFunctions is is set of builtin template function and can be invoked within {{ }}
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class FieldDataFunctions {
	
	private FieldDataFunctions() {}

	//Random value generator
	private static SplittableRandom random = new SplittableRandom();

	//Random timestamp between two dates with input format
	private static final String DEFAULT_INPUT_FORMAT = "dd-MM-yyyy HH:mm:ss";

	//List of pre-defined first name set
	private static String[] FIRST_NAMES = null;

	//List of pre-defined last name set
	private static String[] LAST_NAMES = null;

	//Generate sequence of number for given sequence id
	private static Map<String, AtomicLong> sequenceMap = new ConcurrentHashMap<>();

	private static final Logger log = Logger.getLogger(FieldDataFunctions.class.getName());

	/**
	 * This method returns current timestamp in milliseconds
	 * @return current timestamp in milliseconds
	 */
	public static long TIMESTAMP() {

		return System.currentTimeMillis();

	}

	/**
	 *This method returns random timestamp between two dates
	 * @param beginDate start date for random timestamp in dd-MM-yyyy HH:mm:ss format
	 * @param endDate   end date for random timestamp in dd-MM-yyyy HH:mm:ss format
	 * @return random timestamp between two dates
	 */
	public static long TIMESTAMP(String beginDate, String endDate) {

		try {

			long beginDateTimestamp = DateTimeFormat.forPattern(DEFAULT_INPUT_FORMAT).parseDateTime(beginDate).getMillis();

			long endDateTimestamp = DateTimeFormat.forPattern(DEFAULT_INPUT_FORMAT).parseDateTime(endDate).getMillis();

			return random.nextLong(endDateTimestamp - beginDateTimestamp) + beginDateTimestamp;

		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to parse range date", e);
			throw new IllegalArgumentException(e);

		}
	}

	/**
	 * Returns system date in given input date format
	 *
	 * @param format given date format
	 * @return formatted date
	 */

	public static String DATE(String format) {

		try {

			return DateTime.now().toString(format);

		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to parse current date", e);
			throw new IllegalArgumentException(e);

		}
	}

	/**
	 * Returns random string from the input strings
	 * @param strings
	 * @return random string
	 */
	public static String RANDOM_STRING(String... strings) {

		int randomIndex = random.nextInt(strings.length);

		return strings[randomIndex];

	}

	/**
	 * Returns random integer from the input integers
	 * @param integers
	 * @return random integer
	 */
	public static int RANDOM_INT(int... integers) {

		int randomIndex = random.nextInt(integers.length);

		return integers[randomIndex];

	}

	/**
	 * Returns random float from the input floats
	 * @param floats
	 * @return random float
	 */
	public static float RANDOM_FLOAT(float... floats) {

		int randomIndex = random.nextInt(floats.length);

		return floats[randomIndex];

	}

	/**
	 * Returns random double from the input doubles
	 * @param doubles
	 * @return random double
	 */
	public static double RANDOM_DOUBLE(double... doubles) {

		int randomIndex = random.nextInt(doubles.length);

		return doubles[randomIndex];

	}

	/**
	 * Returns random long from the input integers
	 * @param integers
	 * @return random long
	 */
	public static long RANDOM_LONG(long... integers) {

		int randomIndex = random.nextInt(integers.length);

		return integers[randomIndex];

	}

	/**
	 * Returns random float between given range
	 * @param min minimum float number
	 * @param max maximum float number
	 * @return random float
	 */
	public static float RANDOM_FLOAT_RANGE(float min, float max) {

		return (float) random.nextDouble(min, max);

	}

	/**
	 * Returns random double between given range
	 * @param min minimum double number
	 * @param max maximum double number
	 * @return random double
	 */
	public static double RANDOM_DOUBLE_RANGE(double min, double max) {

		return random.nextDouble(min, max);

	}

	/**
	 * Returns random long between given range
	 * @param min minimum long number
	 * @param max maximum long number
	 * @return random long
	 */
	public static long RANDOM_LONG_RANGE(long min, long max) {

		return random.nextLong(min, max);

	}

	/**
	 * Returns random integer between given range
	 * @param min minimum integer number
	 * @param max maximum integer number
	 * @return random long
	 */
	public static int RANDOM_INT_RANGE(int min, int max) {

		return random.nextInt(min, max);

	}

	/**
	 * Returns random first name from static first names set
	 * @return random first name
	 */
	public static String FIRST_NAME() {

		int randomIndex = random.nextInt(FIRST_NAMES.length);
		return FIRST_NAMES[randomIndex];

	}

	/**
	 * Returns random last name from static first names set
	 * @return random last name
	 */
	public static String LAST_NAME() {

		int randomIndex = random.nextInt(LAST_NAMES.length);
		return LAST_NAMES[randomIndex];

	}

	/**
	 * Returns random last name from static last names set
	 * @return random last name
	 */
	public static String RANDOM_ALPHA_NUMERIC(String charSet, int length) {

		StringBuilder builder = new StringBuilder();
		int charSetLen = charSet.length();

		for (int i = 0; i < length; i++) {
			builder.append(charSet.charAt(random.nextInt(charSetLen)));
		}
		return builder.toString();
	}

	/**
	 *Generates ranom UUID
	 * @return UUID
	 */
	public static String UUID() {

		return UUID.randomUUID().toString();
	}

	/**
	 * Creates new sequence and returns next sequence id
	 * @param sequenceId unique sequence name
	 * @param startValue starting value of sequence
	 * @param incrementBy increment sequence by specific number
	 * @return next sequence number
	 */
	public static long SEQUENCE(String sequenceId, long startValue, long incrementBy) {

		return  sequenceMap.computeIfAbsent(sequenceId, k -> new AtomicLong(startValue)).getAndAdd(incrementBy);

	}

	/**
	 * This method generates random 10 digit random number
	 * @return random 10 digit phone number
	 */
	public static String PHONE() {

		int num1;
		int num2;
		int num3;
		int set2;
		int set3;
		num1 = random.nextInt(7) + 1;
		num2 = random.nextInt(8);
		num3 = random.nextInt(8);
		set2 = random.nextInt(643) + 100;
		set3 = random.nextInt(8999) + 1000;

		return  new StringBuffer("(" ).append( num1 ).append( num2).append( num3 ).append( ")" ).append( "-" ).append( set2 ).append( "-" ).append( set3 ).toString();

	}

	/**
	 * Returns random geneder based on random double value
	 * @return random gender
	 */
	public static String GENDER() {
		return random.nextDouble() > 0.5 ? "male" : "female";
	}

	/**
	 * Returns random boolean based on random boolean value
	 * @return random boolean
	 */
	public static boolean BOOLEAN() {
		return random.nextDouble() > 0.5 ? true : false;
	}

	/**
	 * Generates random email address using random first name and last name
	 * @param domain domain name for which random email addresses will be generated
	 * @return email address
	 */
	public static String EMAIL(String domain) {
		return new StringBuffer(FIRST_NAME()).append(".").append(LAST_NAME()).append("@").append(domain).toString();
	}

	/**
	 *  Generates random username address using random first name and last name
	 * @return random user name
	 */
	public static String USERNAME() {
		return new StringBuffer(FIRST_NAME()).append(".").append(LAST_NAME()).toString();
	}

	/**
	 * Generates random IPV4 address
	 * @return IPV4 address
	 */
	public static String IPV4() {
		String randomIPV4 = null;
		try {
			int randomInt = random.nextInt(Integer.MAX_VALUE);
			byte[] randomBytes = new byte[4];
			for (int i = 0; i < 4; i++) {
				randomBytes[i] = (byte) randomInt;
				randomInt = randomInt >> 8;
			}
			randomIPV4 = Inet4Address.getByAddress(randomBytes).getHostAddress();
		} catch (UnknownHostException e) {
			log.log(Level.SEVERE, "Failed to generate IPV4", e);
		}
		return randomIPV4;
	}

	/**
	 * Generates random IPV6 address
	 * @return IPV6 address
	 */
	public static String IPV6() {
		String randomIPV6 = null;
		try {
			long randomLong = random.nextLong(Long.MAX_VALUE);
			byte[] randomBytes = new byte[16];
			for (int i = 0; i < 16; i++) {
				randomBytes[i] = (byte) randomLong;
				randomLong = randomLong >> 4;
			}
			randomIPV6 = Inet6Address.getByAddress(randomBytes).getHostAddress();
		} catch (UnknownHostException e) {
			log.log(Level.SEVERE, "Failed to generate IPV4", e);
		}
		return randomIPV6;
	}

	/**
	 * Static block loads first names and last names from files
	 */
	static {

		try {

			InputStream firstNameIS = FieldDataFunctions.class.getResourceAsStream("first_names.csv");
			FIRST_NAMES = getNames(firstNameIS);

			InputStream lastNameIS = FieldDataFunctions.class.getResourceAsStream("last_names.csv");
			LAST_NAMES = getNames(lastNameIS);

		} catch (Exception exc) {

			log.log(Level.SEVERE, "failed to load first name, last name CSV files", exc);

		}
	}

	private static String[] getNames(InputStream is) {
		Scanner inputScanner = new Scanner(is);
		List<String> nameList = new ArrayList<>();
		while (inputScanner.hasNextLine()) {
			nameList.add(inputScanner.nextLine());
		}
		try {/*Dummy*/}catch(Exception e){/*Dummy*/}finally {
			inputScanner.close();
		}
		return nameList.toArray(new String[nameList.size()]);
	}
}
