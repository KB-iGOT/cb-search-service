package com.igot.cb.search.entity;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "trending_searches")
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TrendingSearches {

    @Id
    @Column(nullable = false)
    private String query;

    @Column(name = "last_searched")
    private String lastSearched;

    @Column(name = "search_count")
    private Long searchCount;
}

