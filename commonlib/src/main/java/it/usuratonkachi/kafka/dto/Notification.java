package it.usuratonkachi.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.lang.NonNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Notification implements Serializable {
    private static final long serialVersionUID = 8772542218813051755L;

    private String eventTypeId;
    private String producerId;

    private String userId;
    private String archiveId;
    private String docClassId;
    private List<String> listDocClassId;
    private String bucketId;

    private Map<String, String> templateMetadata = new HashMap<>();

    private Map<String, String> attachments = new HashMap<>();
}
