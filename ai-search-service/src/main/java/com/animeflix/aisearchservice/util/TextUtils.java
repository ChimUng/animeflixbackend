package com.animeflix.aisearchservice.util;

/**
 * Utility class để xử lý text (strip HTML, normalize, etc.)
 */
public class TextUtils {

    /**
     * Strip HTML tags từ text
     * Ví dụ: "<p>Hello <b>world</b></p>" → "Hello world"
     */
    public static String stripHtml(String html) {
        if (html == null || html.isEmpty()) {
            return "";
        }

        // Remove HTML tags
        String text = html.replaceAll("<[^>]*>", "");

        // Decode HTML entities
        text = text.replace("&quot;", "\"");
        text = text.replace("&apos;", "'");
        text = text.replace("&amp;", "&");
        text = text.replace("&lt;", "<");
        text = text.replace("&gt;", ">");
        text = text.replace("&#039;", "'");
        text = text.replace("&nbsp;", " ");

        // Remove extra whitespace
        text = text.replaceAll("\\s+", " ").trim();

        return text;
    }

    /**
     * Normalize text cho embedding (lowercase, trim)
     */
    public static String normalize(String text) {
        if (text == null) {
            return "";
        }
        return text.toLowerCase().trim();
    }

    /**
     * Truncate text nếu quá dài
     */
    public static String truncate(String text, int maxLength) {
        if (text == null || text.length() <= maxLength) {
            return text;
        }
        return text.substring(0, maxLength) + "...";
    }

    /**
     * Check xem string có blank không (null hoặc empty)
     */
    public static boolean isBlank(String text) {
        return text == null || text.trim().isEmpty();
    }

    /**
     * Check xem string có dữ liệu không
     */
    public static boolean isNotBlank(String text) {
        return !isBlank(text);
    }
}