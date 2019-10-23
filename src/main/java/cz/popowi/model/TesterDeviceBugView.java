package cz.popowi.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class TesterDeviceBugView {
    private final long testerId;
    private final String firstName;
    private final String lastName;
    private final String country;
    private final Date lastLogin;
    private final long deviceId;
    private final long bugId;
    private final String description;
    private final int count;
}
