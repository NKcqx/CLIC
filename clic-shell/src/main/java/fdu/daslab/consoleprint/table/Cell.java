package fdu.daslab.consoleprint.table;

import fdu.daslab.consoleprint.enums.Align;

/**
 * 引用自：https://github.com/clyoudu/clyoudu-util
 */
public class Cell {

    private Align align;

    private String value;

    public Cell(Align align, String value) {
        this.align = align;
        this.value = value;
    }

    public Cell(String value) {
        this.align = Align.LEFT;
        this.value = value;
    }

    public void setAlign(Align align) {
        this.align = align;
    }

    public Align getAlign() {
        return align;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("{%s: %s,%s: %s}", "value", value, "align", align.name());
    }
}
