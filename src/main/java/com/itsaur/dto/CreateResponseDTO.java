package com.itsaur.dto;

public class CreateResponseDTO {
    private Long id;

    public CreateResponseDTO() {
    }

    public CreateResponseDTO(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
}
