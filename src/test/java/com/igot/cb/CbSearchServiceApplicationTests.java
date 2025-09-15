package com.igot.cb;

import com.igot.cb.search.service.SearchService;
import com.igot.cb.util.CbServerProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@SpringBootTest
class CbSearchServiceApplicationTests {
    @Autowired
    private SearchService searchService;

     @MockBean
     private CbServerProperties cbServerProperties;

    @Test
    void contextLoads() {
        // Just verifies Spring Boot context loads successfully
    }

    @Test
    void searchServiceBeanLoads() {
        assertThat(searchService).isNotNull();
    }

    @Test
    void cbServerPropertiesLoads() {
        assertThat(cbServerProperties).isNotNull();
    }
}
