package com.igot.cb.search.repository;

import com.igot.cb.search.entity.TrendingSearches;
import com.igot.cb.search.entity.UserSearchInputs;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserSearchInputsRepository extends JpaRepository<UserSearchInputs,String> {
}
