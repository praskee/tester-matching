package cz.popowi.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Device {
    private final long deviceId;
    private final String description;
}
