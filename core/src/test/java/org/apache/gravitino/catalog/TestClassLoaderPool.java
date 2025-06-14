package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestClassLoaderPool {

    private ClassLoaderPool classLoaderPool;

    @Mock
    private IsolatedClassLoader mockClassLoader;

    @Mock
    private ClassLoaderPool.ClassLoaderFactory mockFactory;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        classLoaderPool = new ClassLoaderPool(10, 5);
        when(mockFactory.create(anyString(), anyMap())).thenReturn(mockClassLoader);
    }

    @AfterEach
    void tearDown() {
        if (classLoaderPool != null) {
            classLoaderPool.close();
        }
    }

    @Test
    void testGetOrCreateNewClassLoader() {
        // Given
        String provider = "test-provider";
        Map<String, String> conf = Maps.newHashMap();
        conf.put("key1", "value1");

        // When
        IsolatedClassLoader result = classLoaderPool.getOrCreate(provider, conf, mockFactory);

        // Then
        assertNotNull(result);
        assertEquals(mockClassLoader, result);
        verify(mockFactory, times(1)).create(provider, conf);
    }

    @Test
    void testGetOrCreateReuseExistingClassLoader() {
        // Given
        String provider = "test-provider";
        Map<String, String> conf = Maps.newHashMap();
        conf.put("key1", "value1");

        // When - 第一次创建
        IsolatedClassLoader first = classLoaderPool.getOrCreate(provider, conf, mockFactory);
        // When - 第二次获取相同配置
        IsolatedClassLoader second = classLoaderPool.getOrCreate(provider, conf, mockFactory);

        // Then
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(first, second);
        // Factory只应该被调用一次
        verify(mockFactory, times(1)).create(provider, conf);
    }

    @Test
    void testReleaseClassLoader() {
        // Given
        String provider = "test-provider";
        Map<String, String> conf = Maps.newHashMap();
        conf.put("key1", "value1");

        // When
        IsolatedClassLoader classLoader = classLoaderPool.getOrCreate(provider, conf, mockFactory);
        classLoaderPool.release(provider, conf);

        // Then
        assertNotNull(classLoader);
        verify(mockFactory, times(1)).create(provider, conf);
    }

    @Test
    void testConcurrentAccess() throws InterruptedException {
        // Given
        String provider = "concurrent-provider";
        Map<String, String> conf = Maps.newHashMap();
        conf.put("concurrent", "true");
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // When
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    IsolatedClassLoader result = classLoaderPool.getOrCreate(provider, conf, mockFactory);
                    assertNotNull(result);
                } finally {
                    latch.countDown();
                }
            });
        }

        // Then
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        // 即使多线程并发访问，factory也只应该被调用一次
        verify(mockFactory, times(1)).create(provider, conf);

        executor.shutdown();
    }

    @Test
    void testDifferentProvidersCreateDifferentClassLoaders() {
        // Given
        Map<String, String> conf = Maps.newHashMap();
        conf.put("key1", "value1");

        IsolatedClassLoader mockClassLoader2 = mock(IsolatedClassLoader.class);
        when(mockFactory.create("provider1", conf)).thenReturn(mockClassLoader);
        when(mockFactory.create("provider2", conf)).thenReturn(mockClassLoader2);

        // When
        IsolatedClassLoader result1 = classLoaderPool.getOrCreate("provider1", conf, mockFactory);
        IsolatedClassLoader result2 = classLoaderPool.getOrCreate("provider2", conf, mockFactory);

        // Then
        assertNotNull(result1);
        assertNotNull(result2);
        assertNotEquals(result1, result2);
        verify(mockFactory, times(1)).create("provider1", conf);
        verify(mockFactory, times(1)).create("provider2", conf);
    }

    @Test
    void testCriticalPropertiesExtraction() {
        // Given
        Map<String, String> conf1 = Maps.newHashMap();
        conf1.put("package-path", "/path1");
        conf1.put("non-critical", "value1");

        Map<String, String> conf2 = Maps.newHashMap();
        conf2.put("package-path", "/path1");
        conf2.put("non-critical", "value2"); // 不同的非关键属性

        String provider = "test-provider";

        // When
        IsolatedClassLoader result1 = classLoaderPool.getOrCreate(provider, conf1, mockFactory);
        IsolatedClassLoader result2 = classLoaderPool.getOrCreate(provider, conf2, mockFactory);

        // Then - 应该复用相同的ClassLoader，因为关键属性相同
        assertEquals(result1, result2);
        verify(mockFactory, times(1)).create(eq(provider), anyMap());
    }
}