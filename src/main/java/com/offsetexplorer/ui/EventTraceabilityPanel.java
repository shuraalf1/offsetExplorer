package com.offsetexplorer.ui;

import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.model.TraceHopRecord;
import com.offsetexplorer.service.EventTraceabilityService;
import com.offsetexplorer.service.EventTraceabilityService.KeyMatchMode;
import com.offsetexplorer.service.EventTraceabilityService.TraceabilityReport;
import com.offsetexplorer.service.KafkaMessageService;

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
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.table.DefaultTableModel;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Font;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Pestaña 4: trazabilidad de un evento por Key a través de una lista de tópicos.
 */
public class EventTraceabilityPanel extends JPanel {

    private final Supplier<EnvironmentProfile> profileSupplier;
    private final EventTraceabilityService service = new EventTraceabilityService();
    private final JTextField topicsF = new JTextField(50);
    private final JTextField keyF = new JTextField(16);
    private final JComboBox<KeyMatchMode> modeC = new JComboBox<>(KeyMatchMode.values());
    private final JTextField depthF = new JTextField("2000", 6);
    private final JLabel status = new JLabel(" ");
    private final JTextArea summary = new JTextArea(5, 40);
    private final DefaultTableModel model = new DefaultTableModel(
            new String[]{"Salto", "Tópico", "Partición", "Offset", "Timestamp", "Δ Total (ms)", "Δ Salto (ms)", "Key", "Payload"}, 0) {
        @Override
        public boolean isCellEditable(int r, int c) {
            return false;
        }
    };
    private final JTable table = new JTable(model);
    private final MessageDetailPanel detail = new MessageDetailPanel();
    private List<TraceHopRecord> hops = new ArrayList<>();

    public EventTraceabilityPanel(Supplier<EnvironmentProfile> profileSupplier) {
        super(new BorderLayout());
        this.profileSupplier = profileSupplier;

        JPanel bar = new JPanel(new FlowLayout(FlowLayout.LEFT));
        bar.add(new JLabel("Tópicos (coma):"));
        bar.add(topicsF);
        bar.add(new JLabel("Key:"));
        bar.add(keyF);
        bar.add(modeC);
        bar.add(new JLabel("Profundidad/partición:"));
        bar.add(depthF);
        JButton trace = new JButton("Rastrear");
        trace.addActionListener(e -> trace());
        JButton demo = new JButton("Generar flujo demo");
        demo.addActionListener(e -> demo());
        bar.add(trace);
        bar.add(demo);

        summary.setEditable(false);
        summary.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

        table.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) {
                int r = table.getSelectedRow();
                detail.show(r >= 0 && r < hops.size() ? hops.get(r).fullRecord() : null);
            }
        });

        JSplitPane lower = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(table), detail);
        lower.setResizeWeight(0.5);
        JPanel center = new JPanel(new BorderLayout());
        center.add(new JScrollPane(summary), BorderLayout.NORTH);
        center.add(lower, BorderLayout.CENTER);

        add(bar, BorderLayout.NORTH);
        add(center, BorderLayout.CENTER);
        add(status, BorderLayout.SOUTH);
        onProfileChanged();
    }

    /** Recarga la lista de tópicos de trazabilidad del ambiente activo. */
    public void onProfileChanged() {
        EnvironmentProfile p = profileSupplier.get();
        topicsF.setText(p != null ? String.join(",", p.getTraceabilityTopics()) : "");
        model.setRowCount(0);
        hops = new ArrayList<>();
        summary.setText("");
        detail.show(null);
    }

    private List<String> topics() {
        return Arrays.stream(topicsF.getText().split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    private void trace() {
        EnvironmentProfile p = profileSupplier.get();
        String key = keyF.getText().trim();
        if (key.isEmpty() || topics().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Indique la Key y al menos un tópico.");
            return;
        }
        int depth;
        try {
            depth = Integer.parseInt(depthF.getText().trim());
        } catch (NumberFormatException ex) {
            UiUtil.showError(this, ex);
            return;
        }
        KeyMatchMode mode = (KeyMatchMode) modeC.getSelectedItem();
        List<String> topics = topics();
        new SwingWorker<TraceabilityReport, String>() {
            @Override
            protected TraceabilityReport doInBackground() throws Exception {
                return service.traceEventAcrossTopics(p, topics, key, mode, depth, this::publish);
            }

            @Override
            protected void process(List<String> chunks) {
                status.setText(chunks.get(chunks.size() - 1));
            }

            @Override
            protected void done() {
                try {
                    show(get());
                } catch (Exception e) {
                    UiUtil.showError(EventTraceabilityPanel.this, e.getCause() != null ? e.getCause() : e);
                }
            }
        }.execute();
    }

    private void show(TraceabilityReport r) {
        hops = r.timelineHops();
        model.setRowCount(0);
        for (TraceHopRecord h : hops) {
            model.addRow(new Object[]{h.hopNumber(), h.topic(), h.partition(), h.offset(), h.formattedTime(),
                    h.deltaFromStartMs(), h.deltaFromPreviousHopMs(), h.key(), h.payloadPreview()});
        }
        summary.setText("Key: " + r.searchedKey()
                + "\nMensajes escaneados: " + r.totalMessagesScanned() + " en " + r.durationMs() + " ms"
                + "\nTópicos con eventos : " + r.topicsWithMatches()
                + "\nTópicos SIN eventos : " + r.topicsWithoutMatches()
                + "\nSaltos encontrados  : " + hops.size());
        status.setText("Rastreo completado.");
    }

    private void demo() {
        EnvironmentProfile p = profileSupplier.get();
        String key = keyF.getText().trim().isEmpty() ? "ORD-" + System.currentTimeMillis() % 100000 : keyF.getText().trim();
        keyF.setText(key);
        List<String> topics = topics();
        UiUtil.runAsync(this, () -> new KafkaMessageService().publishDemoTraceFlow(p, topics, key),
                n -> status.setText("Flujo demo publicado: " + n + " eventos con Key " + key));
    }
}
