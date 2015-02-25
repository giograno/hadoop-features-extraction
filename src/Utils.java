public class Utils {

	public static boolean isAscii(String string) {
		if (string == null) {
			return false;
		}

		for (int i = 0; i < string.length(); i++) {
			char ch = string.charAt(i);
			if (ch < 32 || ch > 122)
				return false;
		}
		return true;
	}

}
