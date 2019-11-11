package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Line {
    int id;
    String name;
    char[] passwordChars;
    int passwordLength;
    String hashedPassword;
    String hints[];
}
