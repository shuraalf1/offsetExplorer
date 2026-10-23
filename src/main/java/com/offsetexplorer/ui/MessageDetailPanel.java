package com.offsetexplorer.ui;

import com.offsetexplorer.model.KafkaMessageRecord;
import com.offsetexplorer.util.PayloadFormatter;

import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import java.awt.Font;

/**
 * Visor de payload con pestañas JSON / String / Hex / Headers.
 */
public class MessageDetailPanel extends JTabbedPane {

    private final JTextArea json = area();
    private final JTextArea string = area();
    private final JTextArea hex = area();
    private final JTextArea headers = area();

    public MessageDetailPanel() {
        addTab("JSON", new JScrollPane(json));
        addTab("String", new JScrollPane(string));
        addTab("Hex", new JScrollPane(hex));
        addTab("Headers", new JScrollPane(headers));
    }

    public void show(KafkaMessageRecord r) {
        if (r == null) {
            json.setText("");
            string.setText("");
            hex.setText("");
            headers.setText("");
            return;
        }
        json.setText(PayloadFormatter.toPrettyJson(r.stringValue()));
        string.setText(r.stringValue());
        hex.setText(PayloadFormatter.toHexDump(r.rawValue()));
        headers.setText("Key: " + r.key() + "\n\n" + PayloadFormatter.formatHeaders(r.headers()));
        json.setCaretPosition(0);
        string.setCaretPosition(0);
        hex.setCaretPosition(0);
    }

    private static JTextArea area() {
        JTextArea a = new JTextArea();
        a.setEditable(false);
        a.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        return a;
    }
}
