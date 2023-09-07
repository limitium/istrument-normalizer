package com.limitium.gban.usstreetprocessor.common.external;

import org.springframework.stereotype.Component;

@Component
public class ClientKeeper {
    public long lookupBookByPortfolioCode(String portfolioCode) {
        return 0;
    }

    public Book getBookById(long id) {
        return new Book();
    }

    public static class Book {
        public long id;
        public String avpAccount;
    }
}
