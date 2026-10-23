package com.offsetexplorer;

import com.formdev.flatlaf.FlatDarkLaf;
import com.offsetexplorer.ui.MainFrame;

import javax.swing.SwingUtilities;

/**
 * Punto de entrada de Kafka Offset Explorer.
 */
public class MainApp {

    public static void main(String[] args) {
        FlatDarkLaf.setup();
        SwingUtilities.invokeLater(() -> new MainFrame().setVisible(true));
    }
}
