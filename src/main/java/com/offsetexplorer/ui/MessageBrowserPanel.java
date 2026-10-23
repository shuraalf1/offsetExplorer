package com.offsetexplorer.ui;

import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.model.KafkaMessageRecord;
import com.offsetexplorer.service.KafkaAdminService;
import com.offsetexplorer.service.KafkaMessageService;
import com.offsetexplorer.service.KafkaMessageService.SeekMode;
import com.offsetexplorer.util.PayloadFormatter;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.table.DefaultTableModel;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Pestaña 2: lectura de mensajes (JSON/String/Hex) y publicación de mensajes de prueba.
 */
public class MessageBrowserPanel extends JPanel {

    private final Supplier<EnvironmentProfile> profileSupplier;
    private final KafkaAdminService admin = new KafkaAdminService();
    private final KafkaMessageService service = new KafkaMessageService();

    private final JComboBox<String> topicC = new JComboBox<>();
    private final JTextField partitionF = new JTextField("-1", 4);
    private final JComboBox<SeekMode> modeC = new JComboBox<>(SeekMode.values());
    private final JTextField valueF = new JTextField("100", 10);
    private final JTextField maxF = new JTextField("100", 5);
    private final JTextField filterF = new JTextField(14);
    private final JLabel status = new JLabel(" ");
    private final DefaultTableModel model = new DefaultTableModel(
            new String[]{"Partición", "Offset", "Timestamp", "Key", "Bytes", "Preview"}, 0) {
        @Override
        public boolean isCellEditable(int r, int c) {
            return false;
        }
    };
    private final JTable table = new JTable(model);
    private final MessageDetailPanel detail = new MessageDetailPanel();
    private List<KafkaMessageRecord> current = new ArrayList<>();

    public MessageBrowserPanel(Supplier<EnvironmentProfile> profileSupplier) {
        super(new BorderLayout());
        this.profileSupplier = profileSupplier;
        topicC.setEditable(true);
        topicC.setPrototypeDisplayValue("xxxxxxxxxxxxxxxxxxxxxxxxxxxx");

        JPanel bar = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton load = new JButton("Cargar tópicos");
        load.addActionListener(e -> loadTopics());
        JButton fetch = new JButton("Leer mensajes");
        fetch.addActionListener(e -> fetch());
        JButton publish = new JButton("Publicar...");
        publish.addActionListener(e -> publish());
        bar.add(load);
        bar.add(new JLabel("Tópico:"));
        bar.add(topicC);
        bar.add(new JLabel("Partición (-1 = todas):"));
        bar.add(partitionF);
        bar.add(modeC);
        bar.add(new JLabel("Offset/Timestamp:"));
        bar.add(valueF);
        bar.add(new JLabel("Máx:"));
        bar.add(maxF);
        bar.add(new JLabel("Filtro:"));
        bar.add(filterF);
        bar.add(fetch);
        bar.add(publish);

        table.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) {
                int r = table.getSelectedRow();
                detail.show(r >= 0 && r < current.size() ? current.get(table.convertRowIndexToModel(r)) : null);
            }
        });

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(table), detail);
        split.setResizeWeight(0.45);
        add(bar, BorderLayout.NORTH);
        add(split, BorderLayout.CENTER);
        add(status, BorderLayout.SOUTH);
    }

    public void onProfileChanged() {
        topicC.removeAllItems();
        model.setRowCount(0);
        current = new ArrayList<>();
        detail.show(null);
        status.setText("Ambiente cambiado. Pulse 'Cargar tópicos'.");
    }

    private void loadTopics() {
        EnvironmentProfile p = profileSupplier.get();
        UiUtil.runAsync(this, () -> admin.listTopicsWithOffsets(p, false), list -> {
            topicC.removeAllItems();
            list.forEach(t -> topicC.addItem(t.name()));
            status.setText(list.size() + " tópicos cargados.");
        });
    }

    private void fetch() {
        EnvironmentProfile p = profileSupplier.get();
        Object sel = topicC.getEditor().getItem();
        String topic = sel != null ? sel.toString().trim() : "";
        if (topic.isEmpty()) {
            return;
        }
        int part;
        long val;
        int max;
        try {
            part = Integer.parseInt(partitionF.getText().trim());
            val = Long.parseLong(valueF.getText().trim());
            max = Integer.parseInt(maxF.getText().trim());
        } catch (NumberFormatException ex) {
            UiUtil.showError(this, ex);
            return;
        }
        SeekMode mode = (SeekMode) modeC.getSelectedItem();
        String filter = filterF.getText();
        status.setText("Leyendo...");
        UiUtil.runAsync(this, () -> service.fetchMessages(p, topic, part, mode, val, max, filter), res -> {
            current = res;
            model.setRowCount(0);
            for (KafkaMessageRecord r : res) {
                model.addRow(new Object[]{r.partition(), r.offset(), r.formattedTimestamp(), r.key(),
                        r.sizeBytes(), PayloadFormatter.summarize(r.stringValue(), 120)});
            }
            status.setText(res.size() + " mensajes leídos (sin commit de offsets).");
        });
    }

    private void publish() {
        JTextField topic = new JTextField(topicC.getEditor().getItem().toString());
        JTextField key = new JTextField();
        JTextArea value = new JTextArea(8, 40);
        JTextArea headers = new JTextArea(3, 40);
        JPanel form = new JPanel(new GridLayout(0, 1));
        form.add(new JLabel("Tópico"));
        form.add(topic);
        form.add(new JLabel("Key"));
        form.add(key);
        form.add(new JLabel("Value"));
        form.add(new JScrollPane(value));
        form.add(new JLabel("Headers (una línea clave=valor)"));
        form.add(new JScrollPane(headers));
        if (JOptionPane.showConfirmDialog(this, form, "Publicar mensaje",
                JOptionPane.OK_CANCEL_OPTION) != JOptionPane.OK_OPTION) {
            return;
        }
        Map<String, String> h = new LinkedHashMap<>();
        for (String line : headers.getText().split("\\R")) {
            int i = line.indexOf('=');
            if (i > 0) {
                h.put(line.substring(0, i).trim(), line.substring(i + 1).trim());
            }
        }
        EnvironmentProfile p = profileSupplier.get();
        UiUtil.runAsync(this, () -> service.publishMessage(p, topic.getText(), null, key.getText(), value.getText(), h),
                md -> status.setText("Publicado en " + md.topic() + " p" + md.partition() + " offset " + md.offset()));
    }
}
