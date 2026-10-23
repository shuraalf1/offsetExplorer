package com.offsetexplorer.ui;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import java.awt.Component;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * Utilidades de UI: ejecución asíncrona (SwingWorker) para no bloquear el EDT.
 */
public final class UiUtil {

    private UiUtil() {
    }

    /** Ejecuta {@code task} en segundo plano y entrega el resultado (o error) en el EDT. */
    public static <T> void runAsync(Component parent, Callable<T> task, Consumer<T> onSuccess) {
        new SwingWorker<T, Void>() {
            @Override
            protected T doInBackground() throws Exception {
                return task.call();
            }

            @Override
            protected void done() {
                try {
                    onSuccess.accept(get());
                } catch (Exception e) {
                    Throwable c = e.getCause() != null ? e.getCause() : e;
                    showError(parent, c);
                }
            }
        }.execute();
    }

    public static void showError(Component parent, Throwable t) {
        SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(parent,
                t.getClass().getSimpleName() + ": " + t.getMessage(),
                "Error", JOptionPane.ERROR_MESSAGE));
    }
}
