package com.igot.cb.search.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "user_search_inputs")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class UserSearchInputs {
    @Id
    @Column(name = "raw_query", nullable = false)
    private String rawQuery;

    @Column(name = "processed_query", nullable = false)
    private String processedQuery;

    @Column(name = "updated_on", nullable = false)
    private String updatedOn ;

}
