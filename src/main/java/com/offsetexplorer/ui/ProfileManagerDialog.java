package com.offsetexplorer.ui;

import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.model.SaslMechanismType;
import com.offsetexplorer.model.SecurityProtocolType;

import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Diálogo CRUD de ambientes con configuración SASL/SSL y tópicos de trazabilidad.
 */
public class ProfileManagerDialog extends JDialog {

    private final DefaultListModel<EnvironmentProfile> listModel = new DefaultListModel<>();
    private final JList<EnvironmentProfile> list = new JList<>(listModel);

    private final JTextField nameF = new JTextField();
    private final JTextField tagF = new JTextField();
    private final JTextField bootstrapF = new JTextField();
    private final JComboBox<SecurityProtocolType> protocolC = new JComboBox<>(SecurityProtocolType.values());
    private final JComboBox<SaslMechanismType> mechC = new JComboBox<>(SaslMechanismType.values());
    private final JTextField userF = new JTextField();
    private final JPasswordField passF = new JPasswordField();
    private final JTextArea jaasA = new JTextArea(2, 20);
    private final JTextField tsLocF = new JTextField();
    private final JPasswordField tsPassF = new JPasswordField();
    private final JComboBox<String> tsTypeC = new JComboBox<>(new String[]{"JKS", "PKCS12"});
    private final JTextField ksLocF = new JTextField();
    private final JPasswordField ksPassF = new JPasswordField();
    private final JPasswordField keyPassF = new JPasswordField();
    private final JCheckBox noHostVerify = new JCheckBox("Deshabilitar verificación de hostname");
    private final JTextField timeoutF = new JTextField("8000");
    private final JTextField topicsF = new JTextField();

    private EnvironmentProfile editing;

    public ProfileManagerDialog(JFrame owner, List<EnvironmentProfile> profiles) {
        super(owner, "Gestión de ambientes", true);
        profiles.forEach(listModel::addElement);
        list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        list.addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) {
                commitEditing();
                editing = list.getSelectedValue();
                loadForm();
            }
        });

        JScrollPane left = new JScrollPane(list);
        left.setPreferredSize(new Dimension(280, 400));

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton add = new JButton("Nuevo");
        add.addActionListener(e -> {
            commitEditing();
            EnvironmentProfile p = new EnvironmentProfile();
            p.setName("Nuevo ambiente");
            listModel.addElement(p);
            list.setSelectedValue(p, true);
        });
        JButton dup = new JButton("Duplicar");
        dup.addActionListener(e -> {
            commitEditing();
            if (editing == null) {
                return;
            }
            EnvironmentProfile c = copyOf(editing);
            listModel.addElement(c);
            list.setSelectedValue(c, true);
        });
        JButton del = new JButton("Eliminar");
        del.addActionListener(e -> {
            EnvironmentProfile sel = list.getSelectedValue();
            if (sel != null && listModel.size() > 1) {
                editing = null;
                listModel.removeElement(sel);
                list.setSelectedIndex(0);
            }
        });
        JButton close = new JButton("Guardar y cerrar");
        close.addActionListener(e -> {
            commitEditing();
            dispose();
        });
        buttons.add(add);
        buttons.add(dup);
        buttons.add(del);
        buttons.add(close);

        JPanel leftPanel = new JPanel(new BorderLayout());
        leftPanel.add(left, BorderLayout.CENTER);

        add(leftPanel, BorderLayout.WEST);
        add(new JScrollPane(buildForm()), BorderLayout.CENTER);
        add(buttons, BorderLayout.SOUTH);
        setSize(960, 640);
        setLocationRelativeTo(owner);
        if (!listModel.isEmpty()) {
            list.setSelectedIndex(0);
        }
    }

    public List<EnvironmentProfile> getProfiles() {
        List<EnvironmentProfile> out = new ArrayList<>();
        for (int i = 0; i < listModel.size(); i++) {
            out.add(listModel.get(i));
        }
        return out;
    }

    private JPanel buildForm() {
        JPanel p = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(3, 6, 3, 6);
        c.fill = GridBagConstraints.HORIZONTAL;
        int row = 0;
        row = addRow(p, c, row, "Nombre", nameF);
        row = addRow(p, c, row, "Etiqueta (LOCAL/DEV/QA/PROD)", tagF);
        row = addRow(p, c, row, "Bootstrap servers", bootstrapF);
        row = addRow(p, c, row, "Protocolo de seguridad", protocolC);
        row = addRow(p, c, row, "Mecanismo SASL", mechC);
        row = addRow(p, c, row, "Usuario SASL", userF);
        row = addRow(p, c, row, "Password SASL", passF);
        row = addRow(p, c, row, "JAAS personalizado (si CUSTOM)", new JScrollPane(jaasA));
        row = addRow(p, c, row, "Truststore (ruta)", tsLocF);
        row = addRow(p, c, row, "Truststore password", tsPassF);
        row = addRow(p, c, row, "Truststore tipo", tsTypeC);
        row = addRow(p, c, row, "Keystore (ruta, mTLS)", ksLocF);
        row = addRow(p, c, row, "Keystore password", ksPassF);
        row = addRow(p, c, row, "Key password", keyPassF);
        row = addRow(p, c, row, "", noHostVerify);
        row = addRow(p, c, row, "Timeout (ms)", timeoutF);
        addRow(p, c, row, "Tópicos de trazabilidad (separados por coma)", topicsF);
        return p;
    }

    private int addRow(JPanel p, GridBagConstraints c, int row, String label, java.awt.Component comp) {
        c.gridy = row;
        c.gridx = 0;
        c.weightx = 0;
        p.add(new JLabel(label), c);
        c.gridx = 1;
        c.weightx = 1;
        p.add(comp, c);
        return row + 1;
    }

    private void loadForm() {
        if (editing == null) {
            return;
        }
        nameF.setText(editing.getName());
        tagF.setText(editing.getEnvironmentTag());
        bootstrapF.setText(editing.getBootstrapServers());
        protocolC.setSelectedItem(editing.getSecurityProtocol());
        mechC.setSelectedItem(editing.getSaslMechanism());
        userF.setText(editing.getSaslUsername());
        passF.setText(editing.getSaslPassword());
        jaasA.setText(editing.getCustomJaasConfig());
        tsLocF.setText(editing.getSslTruststoreLocation());
        tsPassF.setText(editing.getSslTruststorePassword());
        tsTypeC.setSelectedItem(editing.getSslTruststoreType());
        ksLocF.setText(editing.getSslKeystoreLocation());
        ksPassF.setText(editing.getSslKeystorePassword());
        keyPassF.setText(editing.getSslKeyPassword());
        noHostVerify.setSelected(editing.isDisableSslHostnameVerification());
        timeoutF.setText(String.valueOf(editing.getRequestTimeoutMs()));
        topicsF.setText(String.join(",", editing.getTraceabilityTopics()));
    }

    private void commitEditing() {
        if (editing == null) {
            return;
        }
        editing.setName(nameF.getText().trim());
        editing.setEnvironmentTag(tagF.getText().trim());
        editing.setBootstrapServers(bootstrapF.getText().trim());
        editing.setSecurityProtocol((SecurityProtocolType) protocolC.getSelectedItem());
        editing.setSaslMechanism((SaslMechanismType) mechC.getSelectedItem());
        editing.setSaslUsername(userF.getText());
        editing.setSaslPassword(new String(passF.getPassword()));
        editing.setCustomJaasConfig(jaasA.getText());
        editing.setSslTruststoreLocation(tsLocF.getText());
        editing.setSslTruststorePassword(new String(tsPassF.getPassword()));
        editing.setSslTruststoreType((String) tsTypeC.getSelectedItem());
        editing.setSslKeystoreLocation(ksLocF.getText());
        editing.setSslKeystorePassword(new String(ksPassF.getPassword()));
        editing.setSslKeyPassword(new String(keyPassF.getPassword()));
        editing.setDisableSslHostnameVerification(noHostVerify.isSelected());
        try {
            editing.setRequestTimeoutMs(Integer.parseInt(timeoutF.getText().trim()));
        } catch (NumberFormatException ignored) {
            editing.setRequestTimeoutMs(8000);
        }
        List<String> topics = new ArrayList<>();
        Arrays.stream(topicsF.getText().split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(topics::add);
        editing.setTraceabilityTopics(topics);
        list.repaint();
    }

    private EnvironmentProfile copyOf(EnvironmentProfile s) {
        EnvironmentProfile c = new EnvironmentProfile(s.getName() + " (copia)", s.getEnvironmentTag(),
                s.getBootstrapServers(), s.getSecurityProtocol(), s.getSaslMechanism());
        c.setSaslUsername(s.getSaslUsername());
        c.setSaslPassword(s.getSaslPassword());
        c.setCustomJaasConfig(s.getCustomJaasConfig());
        c.setSslTruststoreLocation(s.getSslTruststoreLocation());
        c.setSslTruststorePassword(s.getSslTruststorePassword());
        c.setSslTruststoreType(s.getSslTruststoreType());
        c.setSslKeystoreLocation(s.getSslKeystoreLocation());
        c.setSslKeystorePassword(s.getSslKeystorePassword());
        c.setSslKeyPassword(s.getSslKeyPassword());
        c.setDisableSslHostnameVerification(s.isDisableSslHostnameVerification());
        c.setRequestTimeoutMs(s.getRequestTimeoutMs());
        c.setTraceabilityTopics(new ArrayList<>(s.getTraceabilityTopics()));
        return c;
    }
}
