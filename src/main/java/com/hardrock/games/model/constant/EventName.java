package com.hardrock.games.model.constant;

public enum EventName {
    UNITY_LOGIN("unity_login"),
    SPIN("spin"),
    UNDEFINED("__UNDEFINED__");
    private String text;

    EventName(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static EventName fromString(String text) {
        for (EventName b : EventName.values()) {
            if (b.text.equalsIgnoreCase(text)) {
                return b;
            }
        }
        return UNDEFINED;
    }
}
