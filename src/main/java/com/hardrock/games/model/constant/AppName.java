package com.hardrock.games.model.constant;

public enum AppName {
    NEVERLAND_CASINO("NC"),
    JACKPOT_PLANET("JP"),
    UNDEFINED("__UNDEFINED__");

    private String text;

    AppName(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static AppName fromString(String text) {
        for (AppName b : AppName.values()) {
            if (b.text.equalsIgnoreCase(text)) {
                return b;
            }
        }
        return UNDEFINED;
    }
}
