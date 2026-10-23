package com.offsetexplorer.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Utilidad para formatear cargas útiles (Payloads) de mensajes de Kafka en formatos:
 * - JSON indentado (Pretty-Print) con validación sintáctica
 * - String plano (UTF-8)
 * - Volcado Hexadecimal + ASCII (Hex Dump)
 * - Resumen de Headers
 */
public final class PayloadFormatter {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private PayloadFormatter() {
    }

    /**
     * Verifica si una cadena representa un objeto o arreglo JSON válido.
     */
    public static boolean isValidJson(String raw) {
        if (raw == null || raw.isBlank()) {
            return false;
        }
        String trimmed = raw.trim();
        if (!(trimmed.startsWith("{") || trimmed.startsWith("["))) {
            return false;
        }
        try {
            MAPPER.readTree(trimmed);
            return true;
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    /**
     * Formatea una cadena como JSON indentado (Pretty-Print).
     * Si el contenido no es JSON válido, devuelve un mensaje indicativo junto con el texto original.
     */
    public static String toPrettyJson(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "<Payload vacío / null>";
        }
        String trimmed = raw.trim();
        try {
            JsonNode tree = MAPPER.readTree(trimmed);
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(tree);
        } catch (Exception e) {
            return "/* [AVISO] El payload no es un JSON válido (" + e.getMessage() + "). Mostrando como String: */\n\n" + raw;
        }
    }

    /**
     * Convierte un arreglo de bytes en una representación Hexadecimal + ASCII clásica de 16 bytes por línea.
     */
    public static String toHexDump(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "<0 bytes>";
        }
        StringBuilder sb = new StringBuilder();
        int length = bytes.length;
        for (int i = 0; i < length; i += 16) {
            sb.append(String.format("%08X  ", i));
            StringBuilder ascii = new StringBuilder();
            for (int j = 0; j < 16; j++) {
                if (i + j < length) {
                    byte b = bytes[i + j];
                    sb.append(String.format("%02X ", b));
                    char c = (b >= 32 && b <= 126) ? (char) b : '.';
                    ascii.append(c);
                } else {
                    sb.append("   ");
                    ascii.append(' ');
                }
                if (j == 7) {
                    sb.append(' ');
                }
            }
            sb.append(" |").append(ascii).append("|\n");
        }
        return sb.toString();
    }

    /**
     * Formatea el mapa de cabeceras (Headers) de un mensaje Kafka para lectura rápida.
     */
    public static String formatHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return "<Sin Headers>";
        }
        StringBuilder sb = new StringBuilder();
        headers.forEach((k, v) -> sb.append(k).append(" : ").append(v).append("\n"));
        return sb.toString();
    }

    /**
     * Genera un resumen compacto de una sola línea del payload para tablas.
     */
    public static String summarize(String raw, int maxLen) {
        if (raw == null) {
            return "<null>";
        }
        String singleLine = raw.replaceAll("\\s+", " ").trim();
        if (singleLine.length() <= maxLen) {
            return singleLine;
        }
        return singleLine.substring(0, maxLen) + "...";
    }

    public static byte[] toUtf8Bytes(String text) {
        return text == null ? null : text.getBytes(StandardCharsets.UTF_8);
    }
}
