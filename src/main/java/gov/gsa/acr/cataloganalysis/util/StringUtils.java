package gov.gsa.acr.cataloganalysis.util;

public class StringUtils {
    public static String globToRegex(String string) {
        StringBuilder buffer = new StringBuilder();

        int z = 0;
        while (z < string.length()) {
            switch (string.charAt(z)) {
                case '*' -> {
                    buffer.append(".*");
                    z++;
                }
                case '?' -> {
                    buffer.append('.');
                    z++;
                }
                case '.' -> {
                    buffer.append("\\.");
                    z++;
                }
                case '\\' -> {
                    buffer.append("\\\\");
                    z++;
                }
                case '[', ']', '^', '$', '(', ')', '{', '}', '+', '|' -> {
                    buffer.append('\\');
                    buffer.append(string.charAt(z));
                    z++;
                }
                default -> {
                    buffer.append(string.charAt(z));
                    z++;
                }
            }
        }

        return buffer.toString();
    }
}
