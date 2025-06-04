package com.igot.cb.util;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
@Getter
@Setter
public class CbServerProperties {
  @Value("${elastic.required.field.trending.search.json.path}")
  private String elasticSearchTrendingJsonPath;

  @Value("${elastic.required.field.recent.search.json.path}")
  private String elasticSearchRecentJsonPath;

  @Value("${non.text.fields}")
  private String nonTextFields;
}
