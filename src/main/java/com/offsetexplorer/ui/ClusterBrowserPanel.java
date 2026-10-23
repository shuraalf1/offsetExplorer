package com.offsetexplorer.ui;

import com.offsetexplorer.model.BrokerInfo;
import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.model.PartitionOffsetInfo;
import com.offsetexplorer.model.TopicSummary;
import com.offsetexplorer.service.KafkaAdminService;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableRowSorter;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import javax.swing.RowFilter;

/**
 * Pestaña 1: brokers, tópicos (filtrables) y particiones con offsets.
 */
public class ClusterBrowserPanel extends JPanel {

    private final Supplier<EnvironmentProfile> profileSupplier;
    private final KafkaAdminService admin = new KafkaAdminService();

    private final DefaultTableModel brokerModel = new DefaultTableModel(
            new String[]{"ID", "Host", "Puerto", "Rack", "Controller"}, 0) {
        @Override
        public boolean isCellEditable(int r, int c) {
            return false;
        }
    };
    private final DefaultTableModel topicModel = new DefaultTableModel(
            new String[]{"Tópico", "Particiones", "Replicación", "Mensajes (aprox.)", "Interno"}, 0) {
        @Override
        public boolean isCellEditable(int r, int c) {
            return false;
        }

        @Override
        public Class<?> getColumnClass(int c) {
            return (c == 1 || c == 2 || c == 3) ? Long.class : String.class;
        }
    };
    private final DefaultTableModel partModel = new DefaultTableModel(
            new String[]{"Partición", "Líder", "Réplicas", "ISR", "Offset inicial", "Offset final", "Mensajes"}, 0) {
        @Override
        public boolean isCellEditable(int r, int c) {
            return false;
        }
    };
    private final JTable topicTable = new JTable(topicModel);
    private final TableRowSorter<DefaultTableModel> sorter = new TableRowSorter<>(topicModel);
    private final JTextField filterF = new JTextField(20);
    private final JCheckBox internalChk = new JCheckBox("Incluir internos");
    private final JLabel status = new JLabel(" ");
    private List<TopicSummary> topics = new ArrayList<>();

    public ClusterBrowserPanel(Supplier<EnvironmentProfile> profileSupplier) {
        super(new BorderLayout());
        this.profileSupplier = profileSupplier;
        topicTable.setRowSorter(sorter);

        JPanel bar = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton refresh = new JButton("Conectar / Refrescar");
        refresh.addActionListener(e -> refresh());
        JButton create = new JButton("Crear tópico");
        create.addActionListener(e -> createTopic());
        JButton delete = new JButton("Eliminar tópico");
        delete.addActionListener(e -> deleteTopic());
        bar.add(refresh);
        bar.add(internalChk);
        bar.add(new JLabel("Filtro:"));
        bar.add(filterF);
        bar.add(create);
        bar.add(delete);
        filterF.getDocument().addDocumentListener(new DocumentListener() {
            public void insertUpdate(DocumentEvent e) { applyFilter(); }
            public void removeUpdate(DocumentEvent e) { applyFilter(); }
            public void changedUpdate(DocumentEvent e) { applyFilter(); }
        });

        topicTable.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) {
                showPartitions();
            }
        });

        JScrollPane brokers = new JScrollPane(new JTable(brokerModel));
        brokers.setBorder(javax.swing.BorderFactory.createTitledBorder("Brokers"));
        JSplitPane lower = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,
                new JScrollPane(topicTable), new JScrollPane(new JTable(partModel)));
        lower.setResizeWeight(0.55);
        JSplitPane main = new JSplitPane(JSplitPane.VERTICAL_SPLIT, brokers, lower);
        main.setResizeWeight(0.2);

        add(bar, BorderLayout.NORTH);
        add(main, BorderLayout.CENTER);
        add(status, BorderLayout.SOUTH);
    }

    public void clear() {
        brokerModel.setRowCount(0);
        topicModel.setRowCount(0);
        partModel.setRowCount(0);
        status.setText("Ambiente cambiado. Pulse 'Conectar / Refrescar'.");
    }

    private void applyFilter() {
        String t = filterF.getText().trim();
        sorter.setRowFilter(t.isEmpty() ? null : RowFilter.regexFilter("(?i)" + java.util.regex.Pattern.quote(t), 0));
    }

    private void refresh() {
        EnvironmentProfile p = profileSupplier.get();
        status.setText("Conectando a " + p.getBootstrapServers() + " ...");
        UiUtil.runAsync(this, () -> {
            List<BrokerInfo> b = admin.listBrokers(p);
            List<TopicSummary> t = admin.listTopicsWithOffsets(p, internalChk.isSelected());
            return new Object[]{b, t};
        }, res -> {
            @SuppressWarnings("unchecked")
            List<BrokerInfo> b = (List<BrokerInfo>) res[0];
            @SuppressWarnings("unchecked")
            List<TopicSummary> t = (List<TopicSummary>) res[1];
            brokerModel.setRowCount(0);
            b.forEach(x -> brokerModel.addRow(new Object[]{x.id(), x.host(), x.port(), x.rack(), x.controller() ? "Sí" : ""}));
            topics = t;
            topicModel.setRowCount(0);
            t.forEach(x -> topicModel.addRow(new Object[]{x.name(), (long) x.partitionCount(),
                    (long) x.replicationFactor(), x.totalMessages(), x.internal() ? "Sí" : ""}));
            status.setText(b.size() + " brokers, " + t.size() + " tópicos.");
        });
    }

    private void showPartitions() {
        partModel.setRowCount(0);
        int row = topicTable.getSelectedRow();
        if (row < 0) {
            return;
        }
        String name = (String) topicModel.getValueAt(topicTable.convertRowIndexToModel(row), 0);
        for (TopicSummary t : topics) {
            if (t.name().equals(name)) {
                for (PartitionOffsetInfo p : t.partitions()) {
                    partModel.addRow(new Object[]{p.partition(), p.leaderHost(), p.replicasCount(), p.isrCount(),
                            p.beginningOffset(), p.endOffset(), p.messageCount()});
                }
            }
        }
    }

    private void createTopic() {
        String name = JOptionPane.showInputDialog(this, "Nombre del tópico:");
        if (name == null || name.isBlank()) {
            return;
        }
        String parts = JOptionPane.showInputDialog(this, "Particiones:", "3");
        String rf = JOptionPane.showInputDialog(this, "Factor de replicación:", "1");
        if (parts == null || rf == null) {
            return;
        }
        EnvironmentProfile p = profileSupplier.get();
        UiUtil.runAsync(this, () -> {
            admin.createTopic(p, name, Integer.parseInt(parts.trim()), Short.parseShort(rf.trim()));
            return true;
        }, ok -> refresh());
    }

    private void deleteTopic() {
        int row = topicTable.getSelectedRow();
        if (row < 0) {
            return;
        }
        String name = (String) topicModel.getValueAt(topicTable.convertRowIndexToModel(row), 0);
        EnvironmentProfile p = profileSupplier.get();
        if (JOptionPane.showConfirmDialog(this,
                "¿Eliminar el tópico '" + name + "' en [" + p.getEnvironmentTag() + "]? Esta acción es irreversible.",
                "Confirmar", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE) != JOptionPane.YES_OPTION) {
            return;
        }
        UiUtil.runAsync(this, () -> {
            admin.deleteTopic(p, name);
            return true;
        }, ok -> refresh());
    }
}
