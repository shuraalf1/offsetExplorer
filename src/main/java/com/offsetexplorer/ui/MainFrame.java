package com.offsetexplorer.ui;

import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.repository.ProfileRepository;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.util.List;

/**
 * Ventana principal: selector de ambiente (multi-ambiente) y pestañas funcionales.
 */
public class MainFrame extends JFrame {

    private final ProfileRepository repository = new ProfileRepository();
    private final JComboBox<EnvironmentProfile> profileCombo = new JComboBox<>();
    private final ClusterBrowserPanel clusterPanel;
    private final MessageBrowserPanel messagePanel;
    private final ConsumerGroupPanel groupPanel;
    private final EventTraceabilityPanel tracePanel;
    private List<EnvironmentProfile> profiles;

    public MainFrame() {
        super("Kafka Offset Explorer & Event Tracer");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setPreferredSize(new Dimension(1280, 800));

        profiles = repository.loadProfiles();
        reloadCombo(null);

        clusterPanel = new ClusterBrowserPanel(this::currentProfile);
        messagePanel = new MessageBrowserPanel(this::currentProfile);
        groupPanel = new ConsumerGroupPanel(this::currentProfile);
        tracePanel = new EventTraceabilityPanel(this::currentProfile);

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 6));
        top.add(new JLabel("Ambiente:"));
        profileCombo.setPreferredSize(new Dimension(520, 28));
        top.add(profileCombo);
        JButton manage = new JButton("Gestionar ambientes...");
        manage.addActionListener(e -> {
            ProfileManagerDialog dlg = new ProfileManagerDialog(this, profiles);
            dlg.setVisible(true);
            profiles = dlg.getProfiles();
            repository.saveProfiles(profiles);
            reloadCombo(currentProfile());
        });
        top.add(manage);

        JTabbedPane tabs = new JTabbedPane();
        tabs.addTab("Clúster y Tópicos", clusterPanel);
        tabs.addTab("Mensajes", messagePanel);
        tabs.addTab("Consumer Groups / Offsets", groupPanel);
        tabs.addTab("Trazabilidad de Eventos", tracePanel);

        // Al cambiar de ambiente se limpian las vistas dependientes del clúster.
        profileCombo.addActionListener(e -> {
            clusterPanel.clear();
            messagePanel.onProfileChanged();
            groupPanel.clear();
            tracePanel.onProfileChanged();
        });

        add(top, BorderLayout.NORTH);
        add(tabs, BorderLayout.CENTER);
        pack();
        setLocationRelativeTo(null);
    }

    private EnvironmentProfile currentProfile() {
        return (EnvironmentProfile) profileCombo.getSelectedItem();
    }

    private void reloadCombo(EnvironmentProfile keep) {
        DefaultComboBoxModel<EnvironmentProfile> model = new DefaultComboBoxModel<>();
        profiles.forEach(model::addElement);
        profileCombo.setModel(model);
        if (keep != null && profiles.contains(keep)) {
            profileCombo.setSelectedItem(keep);
        }
    }
}
