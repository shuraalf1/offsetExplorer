package com.offsetexplorer.ui;

import com.offsetexplorer.model.ConsumerGroupLagInfo;
import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.model.OffsetResetStrategy;
import com.offsetexplorer.service.KafkaAdminService;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.DefaultTableModel;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Pestaña 3: Consumer Groups, lag por partición y reseteo de offsets.
 */
public class ConsumerGroupPanel extends JPanel {

    private final Supplier<EnvironmentProfile> profileSupplier;
    private final KafkaAdminService admin = new KafkaAdminService();
    private final JComboBox<String> groupC = new JComboBox<>();
    private final JLabel status = new JLabel(" ");
    private final DefaultTableModel model = new DefaultTableModel(
            new String[]{"Estado", "Tópico", "Partición", "Offset confirmado", "Inicio", "Fin", "Lag", "Consumidor"}, 0) {
        @Override
        public boolean isCellEditable(int r, int c) {
            return false;
        }
    };
    private final JTable table = new JTable(model);

    public ConsumerGroupPanel(Supplier<EnvironmentProfile> profileSupplier) {
        super(new BorderLayout());
        this.profileSupplier = profileSupplier;
        JPanel bar = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton list = new JButton("Listar grupos");
        list.addActionListener(e -> listGroups());
        JButton lag = new JButton("Ver lag");
        lag.addActionListener(e -> showLag());
        JButton reset = new JButton("Resetear offsets...");
        reset.addActionListener(e -> reset());
        bar.add(list);
        bar.add(new JLabel("Grupo:"));
        groupC.setPrototypeDisplayValue("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        bar.add(groupC);
        bar.add(lag);
        bar.add(reset);
        add(bar, BorderLayout.NORTH);
        add(new JScrollPane(table), BorderLayout.CENTER);
        add(status, BorderLayout.SOUTH);
    }

    public void clear() {
        groupC.removeAllItems();
        model.setRowCount(0);
        status.setText("Ambiente cambiado. Pulse 'Listar grupos'.");
    }

    private void listGroups() {
        EnvironmentProfile p = profileSupplier.get();
        UiUtil.runAsync(this, () -> admin.listConsumerGroupIds(p), ids -> {
            groupC.removeAllItems();
            ids.forEach(groupC::addItem);
            status.setText(ids.size() + " grupos.");
        });
    }

    private void showLag() {
        String g = (String) groupC.getSelectedItem();
        if (g == null) {
            return;
        }
        EnvironmentProfile p = profileSupplier.get();
        UiUtil.runAsync(this, () -> admin.getConsumerGroupLag(p, g), rows -> {
            model.setRowCount(0);
            long total = 0;
            for (ConsumerGroupLagInfo r : rows) {
                model.addRow(new Object[]{r.state(), r.topic(), r.partition(), r.committedOffset(),
                        r.beginningOffset(), r.endOffset(), r.lag(), r.memberHost()});
                total += r.lag();
            }
            status.setText("Lag total del grupo " + g + ": " + total);
        });
    }

    private void reset() {
        String g = (String) groupC.getSelectedItem();
        if (g == null) {
            return;
        }
        int row = table.getSelectedRow();
        JTextField topic = new JTextField(row >= 0 ? (String) model.getValueAt(row, 1) : "");
        JTextField part = new JTextField("-1");
        JComboBox<OffsetResetStrategy> strat = new JComboBox<>(OffsetResetStrategy.values());
        JTextField val = new JTextField("0");
        JPanel form = new JPanel(new GridLayout(0, 1));
        form.add(new JLabel("Tópico"));
        form.add(topic);
        form.add(new JLabel("Partición (-1 = todas)"));
        form.add(part);
        form.add(new JLabel("Estrategia"));
        form.add(strat);
        form.add(new JLabel("Valor (offset, o timestamp en epoch ms / ISO-8601 p.ej. 2026-10-23T12:00:00Z)"));
        form.add(val);
        EnvironmentProfile p = profileSupplier.get();
        if (JOptionPane.showConfirmDialog(this, form,
                "Resetear offsets de '" + g + "' en [" + p.getEnvironmentTag() + "] (el grupo debe estar inactivo)",
                JOptionPane.OK_CANCEL_OPTION, JOptionPane.WARNING_MESSAGE) != JOptionPane.OK_OPTION) {
            return;
        }
        OffsetResetStrategy s = (OffsetResetStrategy) strat.getSelectedItem();
        long v;
        int pt;
        try {
            pt = Integer.parseInt(part.getText().trim());
            String t = val.getText().trim();
            v = (s == OffsetResetStrategy.TIMESTAMP && t.contains("T")) ? Instant.parse(t).toEpochMilli() : Long.parseLong(t);
        } catch (Exception ex) {
            UiUtil.showError(this, ex);
            return;
        }
        UiUtil.runAsync(this, () -> admin.resetConsumerGroupOffsets(p, g, topic.getText().trim(), pt, s, v), res -> {
            status.setText("Offsets reseteados: " + res.size() + " partición(es).");
            showLag();
        });
    }
}
