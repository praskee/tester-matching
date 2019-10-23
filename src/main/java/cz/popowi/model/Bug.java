package cz.popowi.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Bug {
    private final long bugId;
    private final long deviceId;
    private final long testerId;
}
