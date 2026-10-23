package com.offsetexplorer.model;

/**
 * Estrategias disponibles para el reseteo de offsets de un Consumer Group.
 */
public enum OffsetResetStrategy {
    EARLIEST("Earliest (Inicio de la partición)"),
    LATEST("Latest (Final actual de la partición)"),
    SPECIFIC_OFFSET("Offset específico (Número exacto)"),
    TIMESTAMP("Timestamp (Fecha/Hora en epoch ms o ISO)");

    private final String label;

    OffsetResetStrategy(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return label;
    }
}
