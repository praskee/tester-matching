package cz.popowi.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TesterDevice {
    private final long testerId;
    private final long deviceId;
}
