package cz.popowi.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BugDeviceView {
    private final long deviceId;
    private final long bugId;
    private final long testerId;
    private final String description;
}
