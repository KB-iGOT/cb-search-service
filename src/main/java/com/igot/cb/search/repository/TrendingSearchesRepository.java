package com.igot.cb.search.repository;

import com.igot.cb.search.entity.TrendingSearches;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface TrendingSearchesRepository extends JpaRepository<TrendingSearches,String> {
}
