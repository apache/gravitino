package org.apache.gravitino.server.web.ui;

import org.junit.jupiter.api.Test;

import javax.servlet.FilterChain;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;

public class WebUIFilterTest {
    @Test
    public void testNestedDirectoryRequestForwardsToIndexHtml() throws ServletException, IOException {
        WebUIFilter filter = new WebUIFilter();
        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletResponse response = mock(ServletResponse.class);
        FilterChain chain = mock(FilterChain.class);
        RequestDispatcher dispatcher = mock(RequestDispatcher.class);

        when(request.getRequestURI()).thenReturn("/ui/section/subsection/");
        when(request.getRequestDispatcher("/ui/section/subsection/index.html")).thenReturn(dispatcher);

        filter.doFilter(request, response, chain);

        verify(dispatcher).forward(request, response);
        verify(chain, never()).doFilter(any(), any());
    }
}
